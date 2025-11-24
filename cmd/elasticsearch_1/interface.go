package elasticsearch_1

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
)

const IndexName = "rls_resources"

// =========================
// Build and index resource docs
// =========================

func indexResourceDocs(
	ctx context.Context,
	es *elasticsearch.Client,
	resourceOrg map[int]int,
	orgAdmins map[int]intSet,
	orgMembers map[int]intSet,
	groupMembers map[int]intSet,
	directUserManagers map[int]intSet,
	directUserViewers map[int]intSet,
	groupManagers map[int]intSet,
	groupViewers map[int]intSet,
	resourceACL map[int][]aclEntry,
) {
	start := time.Now()
	var buf bytes.Buffer
	docCount := 0

	flush := func() {
		if buf.Len() == 0 {
			return
		}

		bulkCtx, cancel := context.WithTimeout(ctx, esBulkTimeoutSec*time.Second)
		defer cancel()

		res, err := es.Bulk(bytes.NewReader(buf.Bytes()),
			es.Bulk.WithContext(bulkCtx),
		)
		if err != nil {
			log.Fatalf("[elasticsearch_1] bulk index failed: %v", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			log.Fatalf("[elasticsearch_1] bulk index returned error: %s", res.Status())
		}

		buf.Reset()
	}

	for resID, orgID := range resourceOrg {
		manageUsers := make(intSet)
		viewUsers := make(intSet)

		// 1) direct manager users
		if s, ok := directUserManagers[resID]; ok {
			for u := range s {
				manageUsers.add(u)
			}
		}

		// 2) managers via groups
		if mgrGroups, ok := groupManagers[resID]; ok {
			for g := range mgrGroups {
				if members, ok := groupMembers[g]; ok {
					for u := range members {
						manageUsers.add(u)
					}
				}
			}
		}

		// 3) org admins
		if admins, ok := orgAdmins[orgID]; ok {
			for u := range admins {
				manageUsers.add(u)
			}
		}

		// 4) viewUsers starts as manageUsers
		for u := range manageUsers {
			viewUsers.add(u)
		}

		// 5) direct viewers
		if viewers, ok := directUserViewers[resID]; ok {
			for u := range viewers {
				viewUsers.add(u)
			}
		}

		// 6) viewers via groups
		if viewerGroups, ok := groupViewers[resID]; ok {
			for g := range viewerGroups {
				if members, ok := groupMembers[g]; ok {
					for u := range members {
						viewUsers.add(u)
					}
				}
			}
		}

		// 7) org members
		if members, ok := orgMembers[orgID]; ok {
			for u := range members {
				viewUsers.add(u)
			}
		}

		if len(viewUsers) == 0 {
			// No viewers at all, skip indexing this resource.
			continue
		}

		// Convert sets to slices.
		manageSlice := make([]int, 0, len(manageUsers))
		for u := range manageUsers {
			manageSlice = append(manageSlice, u)
		}
		viewSlice := make([]int, 0, len(viewUsers))
		for u := range viewUsers {
			viewSlice = append(viewSlice, u)
		}

		doc := resourceDoc{
			ResourceID:          resID,
			OrgID:               orgID,
			ACL:                 resourceACL[resID],
			AllowedManageUserID: manageSlice,
			AllowedViewUserID:   viewSlice,
		}

		meta := map[string]map[string]interface{}{
			"index": {
				"_index": IndexName,
				"_id":    strconv.Itoa(resID),
			},
		}

		metaBytes, err := json.Marshal(meta)
		if err != nil {
			log.Fatalf("[elasticsearch_1] marshal bulk meta failed: %v", err)
		}
		docBytes, err := json.Marshal(doc)
		if err != nil {
			log.Fatalf("[elasticsearch_1] marshal resourceDoc failed: %v", err)
		}

		buf.Write(metaBytes)
		buf.WriteByte('\n')
		buf.Write(docBytes)
		buf.WriteByte('\n')

		docCount++
		if docCount%esBulkBatchSize == 0 {
			flush()
		}
	}

	flush()
	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[elasticsearch_1] Indexed %d resource docs into %q in %s", docCount, IndexName, elapsed)
}

// =========================
// Clear index documents
// =========================

func ClearIndexDocs(ctx context.Context, es *elasticsearch.Client) {
	ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	body := `{"query":{"match_all":{}}}`

	res, err := es.DeleteByQuery(
		[]string{IndexName},
		strings.NewReader(body),
		es.DeleteByQuery.WithContext(ctxTimeout),
		es.DeleteByQuery.WithConflicts("proceed"),
	)
	if err != nil {
		log.Fatalf("[elasticsearch_1] delete_by_query on %q failed: %v", IndexName, err)
	}
	defer res.Body.Close()

	// If index is missing, DeleteByQuery returns 404; that is acceptable.
	if res.IsError() && !strings.Contains(res.Status(), "404") {
		log.Fatalf("[elasticsearch_1] delete_by_query on %q returned error: %s", IndexName, res.Status())
	}

	log.Printf("[elasticsearch_1] Cleared existing documents in index %q", IndexName)
}
