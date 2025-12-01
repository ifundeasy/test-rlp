package elasticsearch

// Helper types and constants used by indexing utilities.

// intSet is a small helper set for integer IDs.
type intSet map[int]struct{}

func (s intSet) add(v int) {
	s[v] = struct{}{}
}

// aclEntry represents a single ACL assignment used for optional auditing.
type aclEntry struct {
	SubjectType string `json:"subject_type"`
	SubjectID   int    `json:"subject_id"`
	Relation    string `json:"relation"`
}

// resourceDoc is the denormalized document stored in Elasticsearch.
type resourceDoc struct {
	ResourceID          int        `json:"resource_id"`
	OrgID               int        `json:"org_id"`
	ACL                 []aclEntry `json:"acl,omitempty"`
	AllowedManageUserID []int      `json:"allowed_manage_user_id,omitempty"`
	AllowedViewUserID   []int      `json:"allowed_view_user_id,omitempty"`
}

// Bulk helpers defaults; can be tuned via future env wiring if needed.
const (
	esBulkTimeoutSec = 60
	esBulkBatchSize  = 1000
)
