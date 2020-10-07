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

	"github.com/b2wdigital/restQL-golang/v4/pkg/restql"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const mongoPluginName = "MongoDB"

func init() {
	enabledStr := os.Getenv("RESTQL_DATABASE_ENABLED")
	if enabledStr != "" {
		enabled, err := strconv.ParseBool(enabledStr)
		if err != nil {
			fmt.Println("[WARN] mongo database plugin disabled")
			return
		}

		if !enabled {
			fmt.Println("[WARN] mongo database plugin disabled")
			return
		}
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

func (md *mongoDatabase) Name() string {
	return mongoPluginName
}

func NewMongoDatabase(log restql.Logger) (*mongoDatabase, error) {
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

	var q query

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

	return restql.SavedQuery{Text: r.Text, Deprecated: r.Deprecated}, nil
}

func parseMaxTime(timeout time.Duration) time.Duration {
	t := float64(timeout.Nanoseconds())
	maxTime := time.Duration(math.Ceil(t*0.8)) * time.Nanosecond
	return maxTime
}
