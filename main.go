package restql_mongodb

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/b2wdigital/restQL-golang/v5/pkg/restql"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const mongoPluginName = "MongoDB"

func init() {
	if !isDatabaseEnabled() {
		return
	}

	restql.RegisterPlugin(restql.PluginInfo{
		Name: mongoPluginName,
		Type: restql.DatabasePluginType,
		New: func(logger restql.Logger) (restql.Plugin, error) {
			return NewMongoDatabase(logger)
		},
	})
}

type tenant struct {
	Mappings map[string]string
}

type revision struct {
	Text       string
	Deprecated bool
}

type query struct {
	Name      string
	Namespace string
	Size      int
	Revisions []revision
}

type mongoDatabase struct {
	logger          restql.Logger
	client          *mongo.Client
	mappingsTimeout time.Duration
	queryTimeout    time.Duration
	databaseName    string
}

func NewMongoDatabase(log restql.Logger) (restql.DatabasePlugin, error) {
	connectionString := os.Getenv("RESTQL_DATABASE_CONNECTION_STRING")
	if connectionString == "" {
		log.Info("mongo connection string not detected")
		return nil, nil
	}

	envTimeout := os.Getenv("RESTQL_DATABASE_CONNECTION_TIMEOUT")
	timeout, err := time.ParseDuration(envTimeout)
	if err != nil {
		log.Error("failed to parse connection timeout", err)
		return nil, err
	}

	log.Info("starting database connection", "timeout", timeout.String())

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := mongo.Connect(ctx,
		options.Client().ApplyURI(connectionString),
		options.Client().SetConnectTimeout(timeout),
	)
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	log.Info("database connection established", "url", connectionString)

	envMappingTimeout := os.Getenv("RESTQL_DATABASE_MAPPINGS_READ_TIMEOUT")
	mappingTimeout, err := time.ParseDuration(envMappingTimeout)
	if err != nil {
		log.Error("failed to parse mappings read timeout", err)
		return nil, err
	}

	envQueryTimeout := os.Getenv("RESTQL_DATABASE_QUERY_READ_TIMEOUT")
	queryTimeout, err := time.ParseDuration(envQueryTimeout)
	if err != nil {
		log.Error("failed to parse query read timeout", err)
		return nil, err
	}

	databaseName := os.Getenv("RESTQL_DATABASE_NAME")

	return &mongoDatabase{
		logger:          log,
		client:          client,
		mappingsTimeout: mappingTimeout,
		queryTimeout:    queryTimeout,
		databaseName:    databaseName,
	}, nil
}

func (md *mongoDatabase) Name() string {
	return mongoPluginName
}

func (md *mongoDatabase) FindMappingsForTenant(ctx context.Context, tenantId string) ([]restql.Mapping, error) {
	log := restql.GetLogger(ctx)
	mappingsTimeout := md.mappingsTimeout

	var cancel context.CancelFunc
	if mappingsTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, mappingsTimeout)
		defer cancel()
	}
	log.Debug("mappings timeout defined", "timeout", mappingsTimeout)

	maxTime := parseMaxTime(mappingsTimeout)

	var t tenant

	collection := md.client.Database(md.databaseName).Collection("tenant")
	opt := options.FindOne().SetMaxTime(maxTime)
	singleResult := collection.FindOne(ctx, bson.M{"_id": tenantId}, opt)
	err := singleResult.Err()
	switch {
	case err == mongo.ErrNoDocuments:
		log.Error("mappings not found in database", err, "tenant", tenantId)
		return nil, fmt.Errorf("%w: tenant %s", restql.ErrMappingsNotFoundInDatabase, tenantId)
	case err != nil:
		log.Error("database communication failed when fetching mappings", err, "tenant", tenantId)
		return nil, fmt.Errorf("%w: %s", restql.ErrDatabaseCommunicationFailed, err)
	}

	err = singleResult.Decode(&t)
	if err != nil {
		log.Error("failed to decode mappings from database", err, "tenant", tenantId)
		return nil, fmt.Errorf("%w: %s", restql.ErrMappingsNotFoundInDatabase, err)
	}

	i := 0
	result := make([]restql.Mapping, len(t.Mappings))
	for resourceName, url := range t.Mappings {
		mapping, err := restql.NewMapping(resourceName, url)
		if err != nil {
			continue
		}

		result[i] = mapping
		i++
	}

	return result, nil
}

func (md mongoDatabase) FindQuery(ctx context.Context, namespace string, name string, revision int) (restql.SavedQuery, error) {
	log := restql.GetLogger(ctx)

	queryTimeout := md.queryTimeout
	if queryTimeout > 0 {
		ctx, _ = context.WithTimeout(ctx, queryTimeout)
	}
	log.Debug("query timeout defined", "timeout", queryTimeout)

	maxTime := parseMaxTime(queryTimeout)

	collection := md.client.Database(md.databaseName).Collection("query")
	opt := options.FindOne().SetMaxTime(maxTime)
	singleResult := collection.FindOne(ctx, bson.M{"name": name, "namespace": namespace}, opt)
	err := singleResult.Err()
	switch {
	case err == mongo.ErrNoDocuments:
		log.Error("query not found in database", err, "namespace", namespace, "name", name, "revision", revision)
		return restql.SavedQuery{}, restql.ErrQueryNotFoundInDatabase
	case err != nil:
		log.Error("database communication failed when fetching query", err, "namespace", namespace, "name", name, "revision", revision)
		return restql.SavedQuery{}, fmt.Errorf("%w: %s", restql.ErrDatabaseCommunicationFailed, err)
	}

	var q query
	err = singleResult.Decode(&q)
	if err != nil {
		log.Error("failed to decode query from database", err, "namespace", namespace, "name", name, "revision", revision)
		return restql.SavedQuery{}, fmt.Errorf("%w: %s", restql.ErrQueryNotFoundInDatabase, err)
	}

	if q.Size < revision || revision < 0 {
		err := errors.Errorf("invalid revision for query %s/%s: major revision %d, given revision %d", namespace, name, q.Size, revision)

		log.Error("revision not found", err, "namespace", namespace, "name", name, "revision", revision)
		return restql.SavedQuery{}, fmt.Errorf("%w: %s", restql.ErrQueryNotFoundInDatabase, err)
	}

	r := q.Revisions[revision-1]

	return restql.SavedQuery{Name: name, Text: r.Text, Revision: revision}, nil
}

func (md *mongoDatabase) FindAllNamespaces(ctx context.Context) ([]string, error) {
	log := restql.GetLogger(ctx)

	queryTimeout := md.queryTimeout
	if queryTimeout > 0 {
		ctx, _ = context.WithTimeout(ctx, queryTimeout)
	}
	log.Debug("query timeout defined", "timeout", queryTimeout)

	maxTime := parseMaxTime(queryTimeout)

	collection := md.client.Database(md.databaseName).Collection("query")
	opt := options.Distinct().SetMaxTime(maxTime)
	dbResult, err := collection.Distinct(ctx, "namespace", bson.M{}, opt)
	switch {
	case err == mongo.ErrNoDocuments:
		log.Error("no namespace found in database", err)
		return nil, nil
	case err != nil:
		log.Error("database communication failed when fetching query", err)
		return nil, fmt.Errorf("%w: %s", restql.ErrDatabaseCommunicationFailed, err)
	}

	namespace := make([]string, len(dbResult))
	for i, r := range dbResult {
		n, ok := r.(string)
		if !ok {
			return nil, fmt.Errorf("failed to parse namespace to string, value: %v, type %T", r, r)
		}
		namespace[i] = n
	}

	log.Debug("namespaces fetched from database", "namespace", namespace)

	return namespace, nil
}

func (md *mongoDatabase) FindQueriesForNamespace(ctx context.Context, namespace string) (map[string][]restql.SavedQuery, error) {
	log := restql.GetLogger(ctx)

	queryTimeout := md.queryTimeout
	if queryTimeout > 0 {
		ctx, _ = context.WithTimeout(ctx, queryTimeout)
	}
	log.Debug("query timeout defined", "timeout", queryTimeout)

	maxTime := parseMaxTime(queryTimeout)

	collection := md.client.Database(md.databaseName).Collection("query")
	opt := options.Find().SetMaxTime(maxTime)
	cursor, err := collection.Find(ctx, bson.M{"namespace": namespace}, opt)
	switch {
	case err == mongo.ErrNoDocuments:
		log.Error("namespace not found in database", err, "namespace", namespace)
		return nil, restql.ErrNamespaceNotFound
	case err != nil:
		log.Error("database communication failed when fetching query", err, "namespace", namespace)
		return nil, fmt.Errorf("%w: %s", restql.ErrDatabaseCommunicationFailed, err)
	}

	var queries []query
	err = cursor.All(ctx, &queries)
	if err != nil {
		return nil, err
	}

	queriesForNamespace := make(map[string][]restql.SavedQuery)
	for _, q := range queries {
		savedQueries := make([]restql.SavedQuery, len(q.Revisions))
		for i, r := range q.Revisions {
			savedQueries[i] = restql.SavedQuery{
				Name:     q.Name,
				Text:     r.Text,
				Revision: i + 1,
			}
		}

		queriesForNamespace[q.Name] = savedQueries
	}

	log.Debug("namespace queries fetched from database", "queries", queriesForNamespace, "namespace", namespace)

	return queriesForNamespace, nil
}

func (md *mongoDatabase) FindQueryWithAllRevisions(ctx context.Context, namespace string, queryName string) ([]restql.SavedQuery, error) {
	log := restql.GetLogger(ctx)

	queryTimeout := md.queryTimeout
	if queryTimeout > 0 {
		ctx, _ = context.WithTimeout(ctx, queryTimeout)
	}
	log.Debug("query timeout defined", "timeout", queryTimeout)

	maxTime := parseMaxTime(queryTimeout)

	collection := md.client.Database(md.databaseName).Collection("query")
	opt := options.FindOne().SetMaxTime(maxTime)
	singleResult := collection.FindOne(ctx, bson.M{"namespace": namespace, "name": queryName}, opt)
	err := singleResult.Err()
	switch {
	case err == mongo.ErrNoDocuments:
		log.Error("namespace not found in database", err, "namespace", namespace, "name", queryName)
		return nil, restql.ErrNamespaceNotFound
	case err != nil:
		log.Error("database communication failed when fetching query", err, "namespace", namespace, "name", queryName)
		return nil, fmt.Errorf("%w: %s", restql.ErrDatabaseCommunicationFailed, err)
	}

	var q query

	err = singleResult.Decode(&q)
	if err != nil {
		log.Error("failed to decode query from database", err, "namespace", namespace, "name", queryName)
		return nil, fmt.Errorf("%w: %s", restql.ErrQueryNotFoundInDatabase, err)
	}

	queryRevisions := make([]restql.SavedQuery, len(q.Revisions))
	for i, r := range q.Revisions {
		queryRevisions[i] = restql.SavedQuery{
			Name:     q.Name,
			Text:     r.Text,
			Revision: i + 1,
		}
	}

	log.Debug("query revisions fetched from database", "revisions", queryRevisions, "namespace", namespace, "name", queryName)

	return queryRevisions, nil
}

func (md *mongoDatabase) CreateQueryRevision(ctx context.Context, namespace string, queryName string, content string) error {
	log := restql.GetLogger(ctx)

	queryTimeout := md.queryTimeout
	if queryTimeout > 0 {
		ctx, _ = context.WithTimeout(ctx, queryTimeout)
	}
	log.Debug("query timeout defined", "timeout", queryTimeout)

	opts := options.Update().SetUpsert(true)
	collection := md.client.Database(md.databaseName).Collection("query")

	rev := revision{Text: content}
	_, err := collection.UpdateOne(
		ctx,
		bson.M{"namespace": namespace, "name": queryName},
		bson.D{
			{"$inc", bson.M{"size": 1}},
			{"$push", bson.M{"revisions": rev}},
		},
		opts,
	)

	return err
}

func (md *mongoDatabase) FindAllTenants(ctx context.Context) ([]string, error) {
	log := restql.GetLogger(ctx)

	queryTimeout := md.queryTimeout
	if queryTimeout > 0 {
		ctx, _ = context.WithTimeout(ctx, queryTimeout)
	}
	log.Debug("query timeout defined", "timeout", queryTimeout)

	maxTime := parseMaxTime(queryTimeout)

	collection := md.client.Database(md.databaseName).Collection("tenant")
	opt := options.Distinct().SetMaxTime(maxTime)
	dbResult, err := collection.Distinct(ctx, "_id", bson.M{}, opt)
	switch {
	case err == mongo.ErrNoDocuments:
		log.Error("no tenant found in database", err)
		return nil, nil
	case err != nil:
		log.Error("database communication failed when fetching query", err)
		return nil, fmt.Errorf("%w: %s", restql.ErrDatabaseCommunicationFailed, err)
	}

	tenants := make([]string, len(dbResult))
	for i, r := range dbResult {
		n, ok := r.(string)
		if !ok {
			return nil, fmt.Errorf("failed to parse tenant to string, value: %v, type %T", r, r)
		}
		tenants[i] = n
	}

	log.Debug("tenants fetched from database", "tenants", tenants)

	return tenants, nil
}

func (md *mongoDatabase) SetMapping(ctx context.Context, tenantID string, resourceName string, url string) error {
	log := restql.GetLogger(ctx)

	queryTimeout := md.queryTimeout
	if queryTimeout > 0 {
		ctx, _ = context.WithTimeout(ctx, queryTimeout)
	}
	log.Debug("query timeout defined", "timeout", queryTimeout)

	opts := options.Update().SetUpsert(true)
	collection := md.client.Database(md.databaseName).Collection("tenant")

	target := fmt.Sprintf("mappings.%s", resourceName)
	_, err := collection.UpdateOne(
		ctx,
		bson.M{"_id": tenantID},
		bson.D{{"$set", bson.M{target: url}}},
		opts,
	)

	return err
}

func parseMaxTime(timeout time.Duration) time.Duration {
	t := float64(timeout.Nanoseconds())
	maxTime := time.Duration(math.Ceil(t*0.8)) * time.Nanosecond
	return maxTime
}

func isDatabaseEnabled() bool {
	enabledStr := os.Getenv("RESTQL_DATABASE_ENABLED")
	if enabledStr != "" {
		enabled, err := strconv.ParseBool(enabledStr)
		if err != nil {
			fmt.Println("[WARN] mongo database plugin disabled")
			return false
		}

		if !enabled {
			fmt.Println("[WARN] mongo database plugin disabled")
			return false
		}
	}

	return true
}
