package csv

import (
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// High-level dataset configuration (defaults).
// You can override these via environment variables:
//
//	RLP_NUM_ORGS
//	RLP_USERS_PER_ORG             // target "typical" users per org
//	RLP_GROUPS_PER_ORG
//	RLP_RESOURCES_PER_ORG
//	RLP_GROUPS_PER_USER
//	RLP_ADMINS_PER_ORG
//	RLP_MANAGER_USERS_PER_RESOURCE
//	RLP_MANAGER_GROUPS_PER_RESOURCE
//	RLP_VIEWER_USERS_PER_RESOURCE
//	RLP_VIEWER_GROUPS_PER_RESOURCE
//	RLP_AVG_ORGS_PER_USER         // average orgs per user (default 2)
//	RLP_RANDOM_SEED               // optional: fixed random seed for reproducibility
const (
	defaultNumOrgs                 = 16
	defaultUsersPerOrg             = 200
	defaultGroupsPerOrg            = 20
	defaultResourcesPerOrg         = 2000
	defaultGroupsPerUser           = 3
	defaultAdminsPerOrg            = 10
	defaultManagerUsersPerResource = 2
	defaultManagerGroupsPerRes     = 1
	defaultViewerUsersPerResource  = 10
	defaultViewerGroupsPerRes      = 3
	defaultAvgOrgsPerUser          = 2
)

type config struct {
	NumOrgs                 int
	UsersPerOrg             int
	GroupsPerOrg            int
	ResourcesPerOrg         int
	GroupsPerUser           int
	AdminsPerOrg            int
	ManagerUsersPerResource int
	ManagerGroupsPerRes     int
	ViewerUsersPerResource  int
	ViewerGroupsPerRes      int
	AvgOrgsPerUser          int
}

func loadConfig() config {
	cfg := config{
		NumOrgs:                 getEnvInt("RLP_NUM_ORGS", defaultNumOrgs),
		UsersPerOrg:             getEnvInt("RLP_USERS_PER_ORG", defaultUsersPerOrg),
		GroupsPerOrg:            getEnvInt("RLP_GROUPS_PER_ORG", defaultGroupsPerOrg),
		ResourcesPerOrg:         getEnvInt("RLP_RESOURCES_PER_ORG", defaultResourcesPerOrg),
		GroupsPerUser:           getEnvInt("RLP_GROUPS_PER_USER", defaultGroupsPerUser),
		AdminsPerOrg:            getEnvInt("RLP_ADMINS_PER_ORG", defaultAdminsPerOrg),
		ManagerUsersPerResource: getEnvInt("RLP_MANAGER_USERS_PER_RESOURCE", defaultManagerUsersPerResource),
		ManagerGroupsPerRes:     getEnvInt("RLP_MANAGER_GROUPS_PER_RESOURCE", defaultManagerGroupsPerRes),
		ViewerUsersPerResource:  getEnvInt("RLP_VIEWER_USERS_PER_RESOURCE", defaultViewerUsersPerResource),
		ViewerGroupsPerRes:      getEnvInt("RLP_VIEWER_GROUPS_PER_RESOURCE", defaultViewerGroupsPerRes),
		AvgOrgsPerUser:          getEnvInt("RLP_AVG_ORGS_PER_USER", defaultAvgOrgsPerUser),
	}

	// Basic safety clamps.
	if cfg.NumOrgs < 1 {
		cfg.NumOrgs = 1
	}
	if cfg.UsersPerOrg < 1 {
		cfg.UsersPerOrg = 1
	}
	if cfg.GroupsPerOrg < 0 {
		cfg.GroupsPerOrg = 0
	}
	if cfg.ResourcesPerOrg < 0 {
		cfg.ResourcesPerOrg = 0
	}
	if cfg.GroupsPerUser < 0 {
		cfg.GroupsPerUser = 0
	}
	if cfg.AdminsPerOrg < 0 {
		cfg.AdminsPerOrg = 0
	}
	if cfg.ManagerUsersPerResource < 0 {
		cfg.ManagerUsersPerResource = 0
	}
	if cfg.ManagerGroupsPerRes < 0 {
		cfg.ManagerGroupsPerRes = 0
	}
	if cfg.ViewerUsersPerResource < 0 {
		cfg.ViewerUsersPerResource = 0
	}
	if cfg.ViewerGroupsPerRes < 0 {
		cfg.ViewerGroupsPerRes = 0
	}
	if cfg.AvgOrgsPerUser < 1 {
		cfg.AvgOrgsPerUser = 1
	}

	return cfg
}

// getEnvInt reads an int from env, falling back to default if unset or invalid.
func getEnvInt(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return n
}

type csvSinks struct {
	orgsFile         *os.File
	usersFile        *os.File
	groupsFile       *os.File
	orgMembersFile   *os.File
	groupMembersFile *os.File
	resourcesFile    *os.File
	resourceACLFile  *os.File
	orgs             *csv.Writer
	users            *csv.Writer
	groups           *csv.Writer
	orgMembers       *csv.Writer
	groupMembers     *csv.Writer
	resources        *csv.Writer
	resourceACL      *csv.Writer
}

func newCsvSinks(dir string) *csvSinks {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatalf("[csv] failed to load data dir %q: %v", dir, err)
	}

	makeWriter := func(name string) (*os.File, *csv.Writer) {
		full := filepath.Join(dir, name)
		f, err := os.Create(full)
		if err != nil {
			log.Fatalf("[csv] failed to create %s: %v", full, err)
		}
		w := csv.NewWriter(f)
		return f, w
	}

	s := &csvSinks{}
	s.orgsFile, s.orgs = makeWriter("organizations.csv")
	s.usersFile, s.users = makeWriter("users.csv")
	s.groupsFile, s.groups = makeWriter("groups.csv")
	s.orgMembersFile, s.orgMembers = makeWriter("org_memberships.csv")
	s.groupMembersFile, s.groupMembers = makeWriter("group_memberships.csv")
	s.resourcesFile, s.resources = makeWriter("resources.csv")
	s.resourceACLFile, s.resourceACL = makeWriter("resource_acl.csv")

	return s
}

func (s *csvSinks) close() {
	writers := []*csv.Writer{
		s.orgs, s.users, s.groups, s.orgMembers, s.groupMembers, s.resources, s.resourceACL,
	}
	for _, w := range writers {
		if w == nil {
			continue
		}
		w.Flush()
		if err := w.Error(); err != nil {
			log.Fatalf("[csv] csv flush error: %v", err)
		}
	}

	files := []*os.File{
		s.orgsFile, s.usersFile, s.groupsFile,
		s.orgMembersFile, s.groupMembersFile,
		s.resourcesFile, s.resourceACLFile,
	}
	for _, f := range files {
		if f != nil {
			if err := f.Close(); err != nil {
				log.Fatalf("[csv] file close error: %v", err)
			}
		}
	}
}

func writeRow(w *csv.Writer, fields ...string) {
	if err := w.Write(fields); err != nil {
		log.Fatalf("[csv] failed to write csv row %v: %v", fields, err)
	}
}

type idCount struct {
	id    int
	count int
}

// summarizeRelation logs:
// - total A nodes
// - average B per A
// - 3 A with fewest B
// - 3 A with "typical" B (around median)
// - 3 A with most B
func summarizeRelation(name, aLabel, bLabel string, counts map[int]int) {
	if len(counts) == 0 {
		log.Printf("[csv] relation %s (%s -> %s): no data", name, aLabel, bLabel)
		return
	}

	pairs := make([]idCount, 0, len(counts))
	total := 0
	for id, c := range counts {
		pairs = append(pairs, idCount{id: id, count: c})
		total += c
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count == pairs[j].count {
			return pairs[i].id < pairs[j].id
		}
		return pairs[i].count < pairs[j].count
	})

	n := len(pairs)
	avg := float64(total) / float64(n)

	log.Printf("[csv] relation %s (%s -> %s): %d %s; avg %.2f %s per %s",
		name, aLabel, bLabel, n, aLabel, avg, bLabel, aLabel)

	printSamples := func(label string, sample []idCount) {
		log.Printf("[csv]   %s %s:", label, aLabel)
		for _, p := range sample {
			log.Printf("[csv]     %s=%d => %d %s", aLabel, p.id, p.count, bLabel)
		}
	}

	// fewest
	fewN := 3
	if n < fewN {
		fewN = n
	}
	printSamples("fewest", pairs[:fewN])

	// typical (around median)
	midIndices := []int{}
	mid := n / 2
	for delta := -1; delta <= 1; delta++ {
		i := mid + delta
		if i >= 0 && i < n {
			midIndices = append(midIndices, i)
		}
	}
	typ := make([]idCount, 0, len(midIndices))
	seenIdx := make(map[int]struct{}, len(midIndices))
	for _, i := range midIndices {
		if _, ok := seenIdx[i]; ok {
			continue
		}
		seenIdx[i] = struct{}{}
		typ = append(typ, pairs[i])
	}
	printSamples("typical", typ)

	// most
	mostN := 3
	if n < mostN {
		mostN = n
	}
	printSamples("most", pairs[n-mostN:])
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// randInRange returns a random integer in [min, max], enforcing min>=1.
func randInRange(r *rand.Rand, min, max int) int {
	if min < 1 {
		min = 1
	}
	if max < min {
		max = min
	}
	return min + r.Intn(max-min+1)
}

// heterogeneous user caps per org using random ranges:
// - some small orgs (1..smallMax)
// - some normal orgs around UsersPerOrg
// - some large orgs up to ~3x UsersPerOrg
func buildOrgUserCaps(cfg config, r *rand.Rand) (map[int]int, int, int) {
	caps := make(map[int]int, cfg.NumOrgs)
	base := cfg.UsersPerOrg
	if base < 1 {
		base = 1
	}

	totalMemberships := 0
	maxCap := 0

	for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
		t := r.Float64()
		var size int

		smallMax := intMin(50, base/4+5) // always >=1
		normalMin := intMax(1, base/2)
		normalMax := intMax(normalMin, int(float64(base)*1.5))
		largeMin := intMax(1, base)
		largeMax := intMax(largeMin, base*3)

		switch {
		case t < 0.2:
			// small orgs
			size = randInRange(r, 1, smallMax)
		case t < 0.8:
			// normal orgs
			size = randInRange(r, normalMin, normalMax)
		default:
			// large orgs
			size = randInRange(r, largeMin, largeMax)
		}

		if size < 1 {
			size = 1
		}

		caps[orgID] = size
		totalMemberships += size
		if size > maxCap {
			maxCap = size
		}
	}

	return caps, totalMemberships, maxCap
}

// heterogeneous group caps per org using random ranges.
func buildOrgGroupCaps(cfg config, r *rand.Rand) map[int]int {
	caps := make(map[int]int, cfg.NumOrgs)
	base := cfg.GroupsPerOrg
	if base <= 0 {
		for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
			caps[orgID] = 0
		}
		return caps
	}

	for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
		t := r.Float64()
		var size int

		smallMax := intMin(5, base)
		normalMin := intMax(1, base/2)
		normalMax := intMax(normalMin, base)
		largeMin := intMax(1, base)
		largeMax := intMax(largeMin, base*2)

		switch {
		case t < 0.2:
			size = randInRange(r, 1, smallMax)
		case t < 0.8:
			size = randInRange(r, normalMin, normalMax)
		default:
			size = randInRange(r, largeMin, largeMax)
		}

		caps[orgID] = size
	}

	return caps
}

// heterogeneous resource caps per org using random ranges.
func buildOrgResourceCaps(cfg config, r *rand.Rand) map[int]int {
	caps := make(map[int]int, cfg.NumOrgs)
	base := cfg.ResourcesPerOrg
	if base <= 0 {
		for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
			caps[orgID] = 0
		}
		return caps
	}

	for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
		t := r.Float64()
		var size int

		smallMax := intMin(50, base/4+5)
		normalMin := intMax(1, base/2)
		normalMax := intMax(normalMin, base)
		largeMin := intMax(1, base)
		largeMax := intMax(largeMin, base*3)

		switch {
		case t < 0.2:
			size = randInRange(r, 1, smallMax)
		case t < 0.8:
			size = randInRange(r, normalMin, normalMax)
		default:
			size = randInRange(r, largeMin, largeMax)
		}

		caps[orgID] = size
	}

	return caps
}

// pickBenchUsersFromUserResources picks:
// - heavy: user with the largest number of resources
// - regular: user around the median
func pickBenchUsersFromUserResources(counts map[int]int) (heavy int, regular int) {
	if len(counts) == 0 {
		return 0, 0
	}

	type idCount struct {
		id    int
		count int
	}

	pairs := make([]idCount, 0, len(counts))
	for id, c := range counts {
		pairs = append(pairs, idCount{id: id, count: c})
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count == pairs[j].count {
			return pairs[i].id < pairs[j].id
		}
		return pairs[i].count < pairs[j].count
	})

	n := len(pairs)
	heavy = pairs[n-1].id
	regular = pairs[n/2].id // median-ish

	return heavy, regular
}

// CsvCreateData generates relational ACL data into ./data/*.csv
// with a heterogeneous, random graph structure suitable for Zanzibar/RLS benchmarks.
func CsvCreateData() {
	cfg := loadConfig()
	start := time.Now()

	// Random source: from env RLP_RANDOM_SEED if set, else time-based.
	var seed int64
	if seedStr := os.Getenv("RLP_RANDOM_SEED"); seedStr != "" {
		if s, err := strconv.ParseInt(seedStr, 10, 64); err == nil {
			seed = s
		}
	}
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	r := rand.New(rand.NewSource(seed))

	log.Printf("[csv] == Generating CSV data into ./data with config: %+v ==", cfg)
	log.Printf("[csv] using random seed=%d", seed)

	sinks := newCsvSinks("data")
	defer sinks.close()

	// Headers
	writeRow(sinks.orgs, "org_id")
	writeRow(sinks.users, "user_id", "primary_org_id")
	writeRow(sinks.groups, "group_id", "org_id")
	writeRow(sinks.orgMembers, "org_id", "user_id", "role")
	writeRow(sinks.groupMembers, "group_id", "user_id", "role")
	writeRow(sinks.resources, "resource_id", "org_id")
	writeRow(sinks.resourceACL, "resource_id", "subject_type", "subject_id", "relation")

	var (
		userCount            int
		groupCount           int
		orgMembershipCount   int
		groupMembershipCount int
		resourceCount        int
		aclCount             int
	)

	// Relation counters (Zanzibar-style A -> B):
	orgToUsers := make(map[int]int)      // 1. org -> users
	userToOrgs := make(map[int]int)      // 2. user -> orgs
	groupToUsers := make(map[int]int)    // 3. group -> users
	userToGroups := make(map[int]int)    // 4. user -> groups
	userToResources := make(map[int]int) // 5. user -> resources
	resourceToUsers := make(map[int]int) // 6. resource -> users

	// orgID -> []userID, orgID -> []groupID, orgID -> []resourceID
	orgUsers := make(map[int][]int, cfg.NumOrgs)
	orgGroups := make(map[int][]int, cfg.NumOrgs)
	orgResources := make(map[int][]int, cfg.NumOrgs)

	// 0) per-org caps
	orgUserCap, totalMemberships, maxOrgUserCap := buildOrgUserCaps(cfg, r)
	orgGroupCap := buildOrgGroupCaps(cfg, r)
	orgResourceCap := buildOrgResourceCaps(cfg, r)

	// 1) derive totalUsers from totalMemberships & AvgOrgsPerUser
	totalUsers := totalMemberships / cfg.AvgOrgsPerUser
	if totalUsers*cfg.AvgOrgsPerUser < totalMemberships {
		totalUsers++ // ceil
	}
	if totalUsers < maxOrgUserCap {
		totalUsers = maxOrgUserCap
	}
	if totalUsers < 1 {
		totalUsers = 1
	}

	log.Printf("[csv] derived: total_memberships=%d, max_org_users=%d", totalMemberships, maxOrgUserCap)
	log.Printf("[csv] derived total_users=%d (avg_orgs_per_user≈%d)", totalUsers, cfg.AvgOrgsPerUser)

	// 2) organizations
	for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
		writeRow(sinks.orgs, strconv.Itoa(orgID))
		orgToUsers[orgID] = 0
	}

	// 3) users (global user space)
	for userID := 1; userID <= totalUsers; userID++ {
		primaryOrgID := ((userID - 1) % cfg.NumOrgs) + 1
		writeRow(sinks.users, strconv.Itoa(userID), strconv.Itoa(primaryOrgID))
		userCount++
	}

	// 4) org_memberships: random unique users per org
	for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
		targetUsers := orgUserCap[orgID]
		if targetUsers <= 0 {
			continue
		}
		if targetUsers > totalUsers {
			targetUsers = totalUsers
		}

		used := make(map[int]struct{}, targetUsers)
		usersSlice := make([]int, 0, targetUsers)

		for len(usersSlice) < targetUsers {
			userID := r.Intn(totalUsers) + 1
			if _, ok := used[userID]; ok {
				continue
			}
			used[userID] = struct{}{}
			usersSlice = append(usersSlice, userID)
		}

		adminsForOrg := cfg.AdminsPerOrg
		if adminsForOrg > len(usersSlice) {
			adminsForOrg = len(usersSlice)
		}

		for idx, userID := range usersSlice {
			role := "member"
			if idx < adminsForOrg {
				role = "admin"
			}

			writeRow(sinks.orgMembers, strconv.Itoa(orgID), strconv.Itoa(userID), role)
			orgMembershipCount++

			orgToUsers[orgID]++
			userToOrgs[userID]++
			orgUsers[orgID] = append(orgUsers[orgID], userID)
		}
	}

	// Ensure every user has at least 1 org (if somehow missed due to random)
	for userID := 1; userID <= totalUsers; userID++ {
		if userToOrgs[userID] == 0 {
			orgID := ((userID - 1) % cfg.NumOrgs) + 1
			writeRow(sinks.orgMembers, strconv.Itoa(orgID), strconv.Itoa(userID), "member")
			orgMembershipCount++
			orgToUsers[orgID]++
			userToOrgs[userID]++
			orgUsers[orgID] = append(orgUsers[orgID], userID)
		}
	}

	// 5) groups (per-org heterogeneous counts)
	nextGroupID := 1
	for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
		numGroups := orgGroupCap[orgID]
		if numGroups < 0 {
			numGroups = 0
		}
		for i := 0; i < numGroups; i++ {
			groupID := nextGroupID
			nextGroupID++

			writeRow(sinks.groups, strconv.Itoa(groupID), strconv.Itoa(orgID))
			groupCount++
			groupToUsers[groupID] = 0
			orgGroups[orgID] = append(orgGroups[orgID], groupID)
		}
	}

	// 6) group_memberships: random range of groups-per-user
	for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
		usersInOrg := orgUsers[orgID]
		groupsInOrg := orgGroups[orgID]

		if len(usersInOrg) == 0 || len(groupsInOrg) == 0 {
			continue
		}

		numGroupsInOrg := len(groupsInOrg)
		for _, userID := range usersInOrg {
			// groups per user: [1 .. min(numGroupsInOrg, 2*GroupsPerUser+1)]
			maxG := intMin(numGroupsInOrg, cfg.GroupsPerUser*2+1)
			if maxG < 1 {
				continue
			}
			groupsForUser := randInRange(r, 1, maxG)

			usedGroups := make(map[int]struct{}, groupsForUser)
			for len(usedGroups) < groupsForUser {
				gIdx := r.Intn(numGroupsInOrg)
				groupID := groupsInOrg[gIdx]
				if _, ok := usedGroups[groupID]; ok {
					continue
				}
				usedGroups[groupID] = struct{}{}

				writeRow(sinks.groupMembers, strconv.Itoa(groupID), strconv.Itoa(userID), "member")
				groupMembershipCount++

				groupToUsers[groupID]++
				userToGroups[userID]++
			}
		}
	}

	// 7) resources (per-org heterogeneous counts)
	nextResourceID := 1
	for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
		numResources := orgResourceCap[orgID]
		if numResources < 0 {
			numResources = 0
		}
		for i := 0; i < numResources; i++ {
			resourceID := nextResourceID
			nextResourceID++

			writeRow(sinks.resources, strconv.Itoa(resourceID), strconv.Itoa(orgID))
			resourceCount++
			orgResources[orgID] = append(orgResources[orgID], resourceID)
		}
	}

	// 8) resource_acl: random ACL fan-out per resource
	for orgID := 1; orgID <= cfg.NumOrgs; orgID++ {
		usersInOrg := orgUsers[orgID]
		groupsInOrg := orgGroups[orgID]
		resourcesInOrg := orgResources[orgID]

		numUsersInOrg := len(usersInOrg)
		numGroupsInOrg := len(groupsInOrg)

		if len(resourcesInOrg) == 0 || numUsersInOrg == 0 {
			continue
		}

		for _, resourceID := range resourcesInOrg {
			seen := make(map[string]struct{})

			addACL := func(subjectType string, subjectID int, relation string) {
				key := fmt.Sprintf("%s|%d|%s|%d", subjectType, subjectID, relation, resourceID)
				if _, ok := seen[key]; ok {
					return
				}
				seen[key] = struct{}{}
				writeRow(
					sinks.resourceACL,
					strconv.Itoa(resourceID),
					subjectType,
					strconv.Itoa(subjectID),
					relation,
				)
				aclCount++

				if subjectType == "user" {
					userToResources[subjectID]++
					resourceToUsers[resourceID]++
				}
			}

			// manager users: [1 .. min(numUsersInOrg, 2*ManagerUsersPerResource+1)]
			mUmax := intMax(1, cfg.ManagerUsersPerResource*2+1)
			managerUsersCount := randInRange(r, 1, intMin(mUmax, numUsersInOrg))

			// viewer users: [1 .. min(numUsersInOrg, 2*ViewerUsersPerResource)]
			vUmax := intMax(1, cfg.ViewerUsersPerResource*2)
			viewerUsersCount := randInRange(r, 1, intMin(vUmax, numUsersInOrg))

			// manager groups
			managerGroupsCount := 0
			if numGroupsInOrg > 0 && cfg.ManagerGroupsPerRes > 0 {
				mGmax := intMax(1, cfg.ManagerGroupsPerRes*2)
				managerGroupsCount = randInRange(r, 1, intMin(mGmax, numGroupsInOrg))
			}

			// viewer groups
			viewerGroupsCount := 0
			if numGroupsInOrg > 0 && cfg.ViewerGroupsPerRes > 0 {
				vGmax := intMax(1, cfg.ViewerGroupsPerRes*2)
				viewerGroupsCount = randInRange(r, 1, intMin(vGmax, numGroupsInOrg))
			}

			// Manager users
			for i := 0; i < managerUsersCount; i++ {
				uIdx := r.Intn(numUsersInOrg)
				userID := usersInOrg[uIdx]
				addACL("user", userID, "manager")
			}

			// Manager groups
			for i := 0; i < managerGroupsCount; i++ {
				if numGroupsInOrg == 0 {
					break
				}
				gIdx := r.Intn(numGroupsInOrg)
				groupID := groupsInOrg[gIdx]
				addACL("group", groupID, "manager")
			}

			// Viewer users
			for i := 0; i < viewerUsersCount; i++ {
				uIdx := r.Intn(numUsersInOrg)
				userID := usersInOrg[uIdx]
				addACL("user", userID, "viewer")
			}

			// Viewer groups
			for i := 0; i < viewerGroupsCount; i++ {
				if numGroupsInOrg == 0 {
					break
				}
				gIdx := r.Intn(numGroupsInOrg)
				groupID := groupsInOrg[gIdx]
				addACL("group", groupID, "viewer")
			}
		}
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)

	log.Printf("[csv] CSV data generation DONE: elapsed=%s", elapsed)
	log.Printf("[csv] organizations:        %d", cfg.NumOrgs)
	log.Printf("[csv] users (global):       %d", userCount)
	log.Printf("[csv] org_memberships:      %d", orgMembershipCount)
	log.Printf("[csv] groups:               %d", groupCount)
	log.Printf("[csv] group_memberships:    %d", groupMembershipCount)
	log.Printf("[csv] resources:            %d", resourceCount)
	log.Printf("[csv] resource_acl entries: %d", aclCount)

	// Zanzibar-style relation breakdown logs
	summarizeRelation("org->users", "org_id", "users", orgToUsers)
	summarizeRelation("user->orgs", "user_id", "orgs", userToOrgs)
	summarizeRelation("group->users", "group_id", "users", groupToUsers)
	summarizeRelation("user->groups", "user_id", "groups", userToGroups)
	summarizeRelation("user->resources", "user_id", "resources", userToResources)
	summarizeRelation("resource->users", "resource_id", "users", resourceToUsers)

	// Pick bench users for lookup_resources benchmarks
	heavy, regular := pickBenchUsersFromUserResources(userToResources)
	if heavy != 0 {
		log.Printf("[csv] BENCH_LOOKUPRES_MANAGE_USER=%d", heavy)
	}
	if regular != 0 {
		log.Printf("[csv] BENCH_LOOKUPRES_VIEW_USER=%d", regular)
	}
}
