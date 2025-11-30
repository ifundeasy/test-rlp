package elasticsearch

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"test-tls/infrastructure"
)

const (
	dataDir          = "data"
	esBulkBatchSize  = 1000
	esBulkTimeoutSec = 60
)

// intSet is a small helper type for building unique integer sets in memory.
type intSet map[int]struct{}

func (s intSet) add(v int) {
	s[v] = struct{}{}
}

func (s intSet) has(v int) bool {
	_, ok := s[v]
	return ok
}

// aclEntry represents a single ACL edge for a resource, stored as a nested field.
type aclEntry struct {
	Relation    string `json:"relation"`
	SubjectType string `json:"subject_type"`
	SubjectID   int    `json:"subject_id"`
}

// resourceDoc is the Elasticsearch document stored in rlp_resources index.
type resourceDoc struct {
	ResourceID          int        `json:"resource_id"`
	OrgID               int        `json:"org_id"`
	ACL                 []aclEntry `json:"acl"`
	AllowedManageUserID []int      `json:"allowed_user_ids_manage"`
	AllowedViewUserID   []int      `json:"allowed_user_ids_view"`
}

// ElasticsearchCreateData loads the CSV dataset produced by cmd/csv/load_data.go,
// computes the full permission closure per resource, and indexes one document per
// resource into Elasticsearch with a denormalized RLS-friendly structure.
//
// CSV files:
//
//	org_memberships.csv:   org_id,user_id,role        // role in {member,admin}
//	group_memberships.csv: group_id,user_id,role      // role currently always "member"
//	resources.csv:         resource_id,org_id
//	resource_acl.csv:      resource_id,subject_type,subject_id,relation
//
// Permission semantics compiled:
//
//	can_manage(user, resource) =
//	  direct manager_user
//	  OR via manager_groups
//	  OR via org admin
//
//	can_view(user, resource) =
//	  can_manage
//	  OR direct viewer_user
//	  OR via viewer_groups
//	  OR via org member
func ElasticsearchCreateData() {
	rootCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	es, cleanup, err := infrastructure.NewElasticsearchFromEnv(rootCtx)
	if err != nil {
		log.Fatalf("[elasticsearch] NewElasticsearchFromEnv failed: %v", err)
	}
	defer cleanup()

	start := time.Now()
	log.Printf("[elasticsearch] == Loading CSV data into Elasticsearch index %q ==", IndexName)

	// Load membership and ACL graph into memory.
	orgAdmins, orgMembers := loadOrgMemberships()
	groupMembers := loadGroupMemberships()
	resourceOrg := loadResources()
	directUserManagers, directUserViewers, groupManagers, groupViewers, resourceACL := loadResourceACL()

	// Clear existing documents in the index but keep mappings/settings intact.
	ClearIndexDocs(rootCtx, es)

	// Build and index documents per resource.
	indexResourceDocs(rootCtx, es, resourceOrg, orgAdmins, orgMembers, groupMembers, directUserManagers, directUserViewers, groupManagers, groupViewers, resourceACL)

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[elasticsearch] Elasticsearch data load DONE: elapsed=%s", elapsed)
}

// =========================
// CSV helpers
// =========================

func openCSV(name string) (*csv.Reader, *os.File) {
	full := filepath.Join(dataDir, name)
	f, err := os.Open(full)
	if err != nil {
		log.Fatalf("[elasticsearch] open %s failed: %v", full, err)
	}
	r := csv.NewReader(f)
	return r, f
}

func mustAtoi(s, field string) int {
	v, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("[elasticsearch] parse int for %s %q failed: %v", field, s, err)
	}
	return v
}

// =========================
// Load org_memberships
// =========================
//
// org_memberships.csv: org_id,user_id,role
//
// Builds:
//
//	orgAdmins[orgID]  -> set of userID
//	orgMembers[orgID] -> set of userID
func loadOrgMemberships() (map[int]intSet, map[int]intSet) {
	r, f := openCSV("org_memberships.csv")
	defer f.Close()

	// header: org_id,user_id,role
	if _, err := r.Read(); err != nil {
		log.Fatalf("[elasticsearch] read org_memberships header: %v", err)
	}

	orgAdmins := make(map[int]intSet)
	orgMembers := make(map[int]intSet)

	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[elasticsearch] org_memberships: read row failed: %v", err)
		}
		if len(rec) < 3 {
			log.Fatalf("[elasticsearch] org_memberships: invalid row: %#v", rec)
		}

		orgID := mustAtoi(rec[0], "org_memberships.org_id")
		userID := mustAtoi(rec[1], "org_memberships.user_id")
		role := rec[2]

		switch role {
		case "admin":
			s, ok := orgAdmins[orgID]
			if !ok {
				s = make(intSet)
				orgAdmins[orgID] = s
			}
			s.add(userID)
		case "member":
			s, ok := orgMembers[orgID]
			if !ok {
				s = make(intSet)
				orgMembers[orgID] = s
			}
			s.add(userID)
		default:
			log.Fatalf("[elasticsearch] org_memberships: unknown role %q", role)
		}

		count++
	}

	log.Printf("[elasticsearch] org_memberships: loaded %d rows", count)
	return orgAdmins, orgMembers
}

// =========================
// Load group_memberships
// =========================
//
// group_memberships.csv: group_id,user_id,role
//
// Builds:
//
//	groupMembers[groupID] -> set of userID
func loadGroupMemberships() map[int]intSet {
	r, f := openCSV("group_memberships.csv")
	defer f.Close()

	// header: group_id,user_id,role
	if _, err := r.Read(); err != nil {
		log.Fatalf("[elasticsearch] read group_memberships header: %v", err)
	}

	groupMembers := make(map[int]intSet)
	count := 0

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[elasticsearch] group_memberships: read row failed: %v", err)
		}
		if len(rec) < 3 {
			log.Fatalf("[elasticsearch] group_memberships: invalid row: %#v", rec)
		}

		groupID := mustAtoi(rec[0], "group_memberships.group_id")
		userID := mustAtoi(rec[1], "group_memberships.user_id")
		// role := rec[2] // currently unused, assumed "member"

		s, ok := groupMembers[groupID]
		if !ok {
			s = make(intSet)
			groupMembers[groupID] = s
		}
		s.add(userID)

		count++
	}

	log.Printf("[elasticsearch] group_memberships: loaded %d rows", count)
	return groupMembers
}

// =========================
// Load resources
// =========================
//
// resources.csv: resource_id,org_id
//
// Builds:
//
//	resourceOrg[resourceID] -> orgID
func loadResources() map[int]int {
	r, f := openCSV("resources.csv")
	defer f.Close()

	// header: resource_id,org_id
	if _, err := r.Read(); err != nil {
		log.Fatalf("[elasticsearch] read resources header: %v", err)
	}

	resourceOrg := make(map[int]int)
	count := 0

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[elasticsearch] resources: read row failed: %v", err)
		}
		if len(rec) < 2 {
			log.Fatalf("[elasticsearch] resources: invalid row: %#v", rec)
		}

		resID := mustAtoi(rec[0], "resources.resource_id")
		orgID := mustAtoi(rec[1], "resources.org_id")

		resourceOrg[resID] = orgID
		count++
	}

	log.Printf("[elasticsearch] resources: loaded %d rows", count)
	return resourceOrg
}

// =========================
// Load resource_acl
// =========================
//
// resource_acl.csv: resource_id,subject_type,subject_id,relation
//
// Builds:
//
//	directUserManagers[resID] -> set of userID
//	directUserViewers[resID]  -> set of userID
//	groupManagers[resID]      -> set of groupID
//	groupViewers[resID]       -> set of groupID
//	resourceACL[resID]        -> []aclEntry (for nested ACL storage)
func loadResourceACL() (
	map[int]intSet,
	map[int]intSet,
	map[int]intSet,
	map[int]intSet,
	map[int][]aclEntry,
) {
	r, f := openCSV("resource_acl.csv")
	defer f.Close()

	// header: resource_id,subject_type,subject_id,relation
	if _, err := r.Read(); err != nil {
		log.Fatalf("[elasticsearch] read resource_acl header: %v", err)
	}

	directUserManagers := make(map[int]intSet)
	directUserViewers := make(map[int]intSet)
	groupManagers := make(map[int]intSet)
	groupViewers := make(map[int]intSet)
	resourceACL := make(map[int][]aclEntry)

	count := 0

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[elasticsearch] resource_acl: read row failed: %v", err)
		}
		if len(rec) < 4 {
			log.Fatalf("[elasticsearch] resource_acl: invalid row: %#v", rec)
		}

		resID := mustAtoi(rec[0], "resource_acl.resource_id")
		subjectType := rec[1]
		subjectID := mustAtoi(rec[2], "resource_acl.subject_id")
		relation := rec[3]

		resourceACL[resID] = append(resourceACL[resID], aclEntry{
			Relation:    relation,
			SubjectType: subjectType,
			SubjectID:   subjectID,
		})

		switch subjectType {
		case "user":
			switch relation {
			case "manager":
				s, ok := directUserManagers[resID]
				if !ok {
					s = make(intSet)
					directUserManagers[resID] = s
				}
				s.add(subjectID)
			case "viewer":
				s, ok := directUserViewers[resID]
				if !ok {
					s = make(intSet)
					directUserViewers[resID] = s
				}
				s.add(subjectID)
			default:
				log.Fatalf("[elasticsearch] resource_acl: unknown relation for user: %q", relation)
			}

		case "group":
			switch relation {
			case "manager":
				s, ok := groupManagers[resID]
				if !ok {
					s = make(intSet)
					groupManagers[resID] = s
				}
				s.add(subjectID)
			case "viewer":
				s, ok := groupViewers[resID]
				if !ok {
					s = make(intSet)
					groupViewers[resID] = s
				}
				s.add(subjectID)
			default:
				log.Fatalf("[elasticsearch] resource_acl: unknown relation for group: %q", relation)
			}

		default:
			log.Fatalf("[elasticsearch] resource_acl: unknown subject_type: %q", subjectType)
		}

		count++
	}

	log.Printf("[elasticsearch] resource_acl: loaded %d rows", count)
	return directUserManagers, directUserViewers, groupManagers, groupViewers, resourceACL
}
