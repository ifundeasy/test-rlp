package elasticsearch

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	esv9 "github.com/elastic/go-elasticsearch/v9"

	"test-tls/infrastructure"
)

const (
	esDataDir = "data"
)

// ElasticsearchCreateData builds effective permission documents and bulk indexes
// them into Elasticsearch index defined in create_schemas.go. Logging mirrors
// cmd/authzed_crdb/load_data.go style and bulk operations overwrite by _id.
func ElasticsearchCreateData() {
	ctx := context.Background()
	es, cleanup, err := infrastructure.NewElasticsearchFromEnv(ctx)
	if err != nil {
		log.Fatalf("[elasticsearch] create client: %v", err)
	}
	defer cleanup()

	start := time.Now()
	log.Printf("[elasticsearch] == Starting Elasticsearch data import from CSV in %q ==", esDataDir)

	// Ensure index exists
	ElasticsearchCreateSchemas()

	// Ingest CSVs into in-memory structures
	resourceOrg := loadResourcesCSV()
	orgAdmins, orgMembers := loadOrgMembershipsCSV()
	groupDirectMembers, groupDirectManagers := loadGroupMembershipsCSV()
	groupHierarchy := loadGroupHierarchyCSV()
	directUserManagers, directUserViewers, groupManagers, groupViewers, resourceACL := loadResourceACLCsv()

	// Precompute effective managers and members per group (with memoization)
	effManagers, effMembers := precomputeEffectiveGroupSets(groupDirectMembers, groupDirectManagers, groupHierarchy)

	// Build and index resource docs
	indexPermissionDocs(ctx, es, resourceOrg, orgAdmins, orgMembers, effManagers, effMembers, directUserManagers, directUserViewers, groupManagers, groupViewers, resourceACL)

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[elasticsearch] Elasticsearch data import DONE: elapsed=%s", elapsed)
}

// ===== CSV helpers =====

func openCSV(name string) (*csv.Reader, *os.File) {
	full := filepath.Join(esDataDir, name)
	f, err := os.Open(full)
	if err != nil {
		log.Fatalf("[elasticsearch] open %s: %v", full, err)
	}
	r := csv.NewReader(f)
	return r, f
}

func atoiStrict(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("[elasticsearch] invalid int %q: %v", s, err)
	}
	return n
}

func loadResourcesCSV() map[int]int {
	r, f := openCSV("resources.csv")
	defer f.Close()
	if _, err := r.Read(); err != nil {
		log.Fatalf("[elasticsearch] read resources header: %v", err)
	}
	m := make(map[int]int)
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[elasticsearch] read resources row: %v", err)
		}
		if len(rec) < 2 {
			log.Fatalf("[elasticsearch] invalid resources row: %#v", rec)
		}
		resID := atoiStrict(rec[0])
		orgID := atoiStrict(rec[1])
		m[resID] = orgID
		count++
		if count%100000 == 0 {
			log.Printf("[elasticsearch] Loaded resources progress: %d rows", count)
		}
	}
	log.Printf("[elasticsearch] Loaded resources: %d rows", count)
	return m
}

func loadOrgMembershipsCSV() (map[int]intSet, map[int]intSet) {
	r, f := openCSV("org_memberships.csv")
	defer f.Close()
	if _, err := r.Read(); err != nil {
		log.Fatalf("[elasticsearch] read org_memberships header: %v", err)
	}
	admins := make(map[int]intSet)
	members := make(map[int]intSet)
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[elasticsearch] read org_memberships row: %v", err)
		}
		if len(rec) < 3 {
			log.Fatalf("[elasticsearch] invalid org_memberships row: %#v", rec)
		}
		orgID := atoiStrict(rec[0])
		userID := atoiStrict(rec[1])
		role := rec[2]
		switch role {
		case "admin":
			s := admins[orgID]
			if s == nil {
				s = make(intSet)
			}
			s.add(userID)
			admins[orgID] = s
		default:
			s := members[orgID]
			if s == nil {
				s = make(intSet)
			}
			s.add(userID)
			members[orgID] = s
		}
		count++
		if count%100000 == 0 {
			log.Printf("[elasticsearch] Loaded org_memberships progress: %d rows", count)
		}
	}
	log.Printf("[elasticsearch] Loaded org_memberships: %d rows", count)
	return admins, members
}

func loadGroupMembershipsCSV() (map[int]intSet, map[int]intSet) {
	r, f := openCSV("group_memberships.csv")
	defer f.Close()
	if _, err := r.Read(); err != nil {
		log.Fatalf("[elasticsearch] read group_memberships header: %v", err)
	}
	directMembers := make(map[int]intSet)
	directManagers := make(map[int]intSet)
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[elasticsearch] read group_memberships row: %v", err)
		}
		if len(rec) < 3 {
			log.Fatalf("[elasticsearch] invalid group_memberships row: %#v", rec)
		}
		groupID := atoiStrict(rec[0])
		userID := atoiStrict(rec[1])
		role := rec[2]
		switch role {
		case "direct_manager", "admin":
			s := directManagers[groupID]
			if s == nil {
				s = make(intSet)
			}
			s.add(userID)
			directManagers[groupID] = s
		default:
			s := directMembers[groupID]
			if s == nil {
				s = make(intSet)
			}
			s.add(userID)
			directMembers[groupID] = s
		}
		count++
		if count%100000 == 0 {
			log.Printf("[elasticsearch] Loaded group_memberships progress: %d rows", count)
		}
	}
	log.Printf("[elasticsearch] Loaded group_memberships: %d rows", count)
	return directMembers, directManagers
}

func loadGroupHierarchyCSV() map[int]map[int]string {
	full := filepath.Join(esDataDir, "group_hierarchy.csv")
	f, err := os.Open(full)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[elasticsearch] group_hierarchy.csv not found, skipping nested groups")
			return map[int]map[int]string{}
		}
		log.Fatalf("[elasticsearch] open group_hierarchy.csv: %v", err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil {
		log.Fatalf("[elasticsearch] read group_hierarchy header: %v", err)
	}
	m := make(map[int]map[int]string)
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[elasticsearch] read group_hierarchy row: %v", err)
		}
		if len(rec) < 3 {
			log.Fatalf("[elasticsearch] invalid group_hierarchy row: %#v", rec)
		}
		parent := atoiStrict(rec[0])
		child := atoiStrict(rec[1])
		relation := rec[2]
		cs := m[parent]
		if cs == nil {
			cs = make(map[int]string)
		}
		cs[child] = relation
		m[parent] = cs
		count++
		if count%100000 == 0 {
			log.Printf("[elasticsearch] Loaded group_hierarchy progress: %d rows", count)
		}
	}
	log.Printf("[elasticsearch] Loaded group_hierarchy: %d rows", count)
	return m
}

func loadResourceACLCsv() (map[int]intSet, map[int]intSet, map[int]intSet, map[int]intSet, map[int][]aclEntry) {
	r, f := openCSV("resource_acl.csv")
	defer f.Close()
	if _, err := r.Read(); err != nil {
		log.Fatalf("[elasticsearch] read resource_acl header: %v", err)
	}
	directManagers := make(map[int]intSet)
	directViewers := make(map[int]intSet)
	groupManagers := make(map[int]intSet)
	groupViewers := make(map[int]intSet)
	acl := make(map[int][]aclEntry)
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[elasticsearch] read resource_acl row: %v", err)
		}
		if len(rec) < 4 {
			log.Fatalf("[elasticsearch] invalid resource_acl row: %#v", rec)
		}
		resID := atoiStrict(rec[0])
		subjectType := rec[1]
		subjectID := atoiStrict(rec[2])
		relation := rec[3]

		// save audit entry
		acl[resID] = append(acl[resID], aclEntry{SubjectType: subjectType, SubjectID: subjectID, Relation: relation})

		switch subjectType {
		case "user":
			switch relation {
			case "manager_user", "manager":
				s := directManagers[resID]
				if s == nil {
					s = make(intSet)
				}
				s.add(subjectID)
				directManagers[resID] = s
			case "viewer_user", "viewer":
				s := directViewers[resID]
				if s == nil {
					s = make(intSet)
				}
				s.add(subjectID)
				directViewers[resID] = s
			default:
				log.Fatalf("[elasticsearch] unknown ACL relation for user: %q", relation)
			}
		case "group":
			switch relation {
			case "manager_group", "manager":
				s := groupManagers[resID]
				if s == nil {
					s = make(intSet)
				}
				s.add(subjectID)
				groupManagers[resID] = s
			case "viewer_group", "viewer":
				s := groupViewers[resID]
				if s == nil {
					s = make(intSet)
				}
				s.add(subjectID)
				groupViewers[resID] = s
			default:
				log.Fatalf("[elasticsearch] unknown ACL relation for group: %q", relation)
			}
		default:
			log.Fatalf("[elasticsearch] unknown subject_type in resource_acl: %q", subjectType)
		}

		count++
		if count%100000 == 0 {
			log.Printf("[elasticsearch] Loaded resource_acl progress: %d rows", count)
		}
	}
	log.Printf("[elasticsearch] Loaded resource_acl: %d rows", count)
	return directManagers, directViewers, groupManagers, groupViewers, acl
}

// ===== Effective group computation =====

func precomputeEffectiveGroupSets(
	directMembers map[int]intSet,
	directManagers map[int]intSet,
	groupHierarchy map[int]map[int]string,
) (map[int]intSet, map[int]intSet) {
	// Memoized DFS
	manMemo := make(map[int]intSet)
	memMemo := make(map[int]intSet)

	var computeManagers func(int) intSet
	var computeMembers func(int) intSet

	computeManagers = func(groupID int) intSet {
		if s, ok := manMemo[groupID]; ok {
			return s
		}
		result := make(intSet)
		if dm, ok := directManagers[groupID]; ok {
			for u := range dm {
				result.add(u)
			}
		}
		if children, ok := groupHierarchy[groupID]; ok {
			for childID, rel := range children {
				if rel == "manager_group" {
					for u := range computeManagers(childID) {
						result.add(u)
					}
				}
			}
		}
		manMemo[groupID] = result
		return result
	}

	computeMembers = func(groupID int) intSet {
		if s, ok := memMemo[groupID]; ok {
			return s
		}
		result := make(intSet)
		if dm, ok := directMembers[groupID]; ok {
			for u := range dm {
				result.add(u)
			}
		}
		if children, ok := groupHierarchy[groupID]; ok {
			for childID, rel := range children {
				if rel == "member_group" {
					for u := range computeMembers(childID) {
						result.add(u)
					}
				}
			}
		}
		// include managers as members
		for u := range computeManagers(groupID) {
			result.add(u)
		}
		memMemo[groupID] = result
		return result
	}

	// Warm caches for all referenced groups
	allGroups := make(map[int]struct{})
	for g := range directMembers {
		allGroups[g] = struct{}{}
	}
	for g := range directManagers {
		allGroups[g] = struct{}{}
	}
	for parent, children := range groupHierarchy {
		allGroups[parent] = struct{}{}
		for child := range children {
			allGroups[child] = struct{}{}
		}
	}
	for g := range allGroups {
		_ = computeManagers(g)
		_ = computeMembers(g)
	}

	return manMemo, memMemo
}

// ===== Bulk indexing =====

func indexPermissionDocs(
	ctx context.Context,
	es *esv9.Client,
	resourceOrg map[int]int,
	orgAdmins map[int]intSet,
	orgMembers map[int]intSet,
	effManagers map[int]intSet,
	effMembers map[int]intSet,
	directUserManagers map[int]intSet,
	directUserViewers map[int]intSet,
	groupManagers map[int]intSet,
	groupViewers map[int]intSet,
	resourceACL map[int][]aclEntry,
) {
	// Optional: clear old docs to avoid stale permissions
	ClearIndexDocs(ctx, es)

	// Stable ordering for predictable logging
	resourceIDs := make([]int, 0, len(resourceOrg))
	for id := range resourceOrg {
		resourceIDs = append(resourceIDs, id)
	}
	sort.Ints(resourceIDs)

	start := time.Now()
	var buf bytes.Buffer
	docCount := 0

	flush := func() {
		if buf.Len() == 0 {
			return
		}
		bulkCtx, cancel := context.WithTimeout(ctx, esBulkTimeoutSec*time.Second)
		defer cancel()
		res, err := es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithContext(bulkCtx), es.Bulk.WithRefresh("false"))
		if err != nil {
			log.Fatalf("[elasticsearch] bulk index failed: %v", err)
		}
		defer res.Body.Close()
		if res.IsError() {
			log.Fatalf("[elasticsearch] bulk index returned error: %s", res.Status())
		}
		buf.Reset()
	}

	// Iterate resources and build effective permission arrays
	for _, resID := range resourceIDs {
		orgID := resourceOrg[resID]

		manage := make(intSet)
		view := make(intSet)

		if s, ok := directUserManagers[resID]; ok {
			for u := range s {
				manage.add(u)
			}
		}
		if gs, ok := groupManagers[resID]; ok {
			for g := range gs {
				if em, ok := effManagers[g]; ok {
					for u := range em {
						manage.add(u)
					}
				}
			}
		}
		if admins, ok := orgAdmins[orgID]; ok {
			for u := range admins {
				manage.add(u)
			}
		}

		for u := range manage {
			view.add(u)
		}
		if s, ok := directUserViewers[resID]; ok {
			for u := range s {
				view.add(u)
			}
		}
		if gs, ok := groupViewers[resID]; ok {
			for g := range gs {
				if em, ok := effMembers[g]; ok {
					for u := range em {
						view.add(u)
					}
				}
			}
		}
		if members, ok := orgMembers[orgID]; ok {
			for u := range members {
				view.add(u)
			}
		}

		if len(view) == 0 {
			continue
		}

		manageSlice := make([]int, 0, len(manage))
		for u := range manage {
			manageSlice = append(manageSlice, u)
		}
		viewSlice := make([]int, 0, len(view))
		for u := range view {
			viewSlice = append(viewSlice, u)
		}

		doc := resourceDoc{
			ResourceID:          resID,
			OrgID:               orgID,
			ACL:                 resourceACL[resID],
			AllowedManageUserID: manageSlice,
			AllowedViewUserID:   viewSlice,
		}

		// Bulk action lines
		// Use the same metadata format as interface.go but inline here
		buf.WriteString(`{"index":{"_index":"` + IndexName + `","_id":"` + strconv.Itoa(resID) + `"}}`)
		buf.WriteByte('\n')
		// encode doc
		b, _ := json.Marshal(doc)
		buf.Write(b)
		buf.WriteByte('\n')

		docCount++
		if docCount%esBulkBatchSize == 0 {
			flush()
		}
		if docCount%100000 == 0 {
			log.Printf("[elasticsearch] Indexed progress: %d resources elapsed=%s", docCount, time.Since(start).Truncate(time.Millisecond))
		}
	}
	flush()

	// Refresh for visibility in benchmarks
	refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := es.Indices.Refresh(es.Indices.Refresh.WithIndex(IndexName), es.Indices.Refresh.WithContext(refreshCtx)); err != nil {
		log.Printf("[elasticsearch] index refresh failed: %v", err)
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[elasticsearch] Indexed %d resource docs into %q in %s", docCount, IndexName, elapsed)
}
