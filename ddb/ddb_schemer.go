package ddb

import (
	"context"
	. "github.com/Cyberax/go-dd-service-base/visibility"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"strings"
	"time"
)

type DynamoDbSchemer struct {
	Suffix    string
	AwsConfig aws.Config
	TestMode  bool
}

func NewDynamoDbSchemer(suffix string, config aws.Config, testMode bool) *DynamoDbSchemer {
	return &DynamoDbSchemer{
		Suffix:    suffix,
		AwsConfig: config,
		TestMode:  testMode,
	}
}

type Table struct {
	Name         string
	HashKeyName  string
	RangeKeyName string
	TtlFieldName string
	GSI          map[string]string
}

func (db *DynamoDbSchemer) InitSchema(ctx context.Context, tablesToCreate []Table) error {
	CL(ctx).Info("Describing tables")

	svc := dynamodb.New(db.AwsConfig)

	var tables = make(map[string]int64)
	lti := dynamodb.ListTablesInput{}
	for {
		output, err := svc.ListTablesRequest(&lti).Send(ctx)
		if err != nil {
			return err
		}

		for _, t := range output.TableNames {
			tables[strings.TrimSuffix(t, db.Suffix)] = 1
		}

		if output.LastEvaluatedTableName == nil {
			break
		}
		lti.ExclusiveStartTableName = output.LastEvaluatedTableName
	}

	// Now create the missing tables
	for _, t := range tablesToCreate {
		if _, ok := tables[t.Name]; ok {
			CLS(ctx).Infof("Table %s exists", t.Name)
			err := db.ensureTtlIsSet(ctx, svc, t.Name+db.Suffix, t.TtlFieldName)
			if err != nil {
				return err
			}
			err = db.ensureGsiIsCreated(ctx, svc, t.Name+db.Suffix, t.GSI)
			if err != nil {
				return err
			}
			continue
		}

		newTableName := t.Name + db.Suffix

		CLS(ctx).Infof("Creating table: %s", newTableName)

		attrDefs := []dynamodb.AttributeDefinition{{
			AttributeName: aws.String(t.HashKeyName), AttributeType: "S"},
		}
		keySchema := []dynamodb.KeySchemaElement{{
			AttributeName: aws.String(t.HashKeyName), KeyType: "HASH",
		}}

		if t.RangeKeyName != "" {
			attrDefs = append(attrDefs, dynamodb.AttributeDefinition{
				AttributeName: aws.String(t.RangeKeyName), AttributeType: "S"})
			keySchema = append(keySchema, dynamodb.KeySchemaElement{
				AttributeName: aws.String(t.RangeKeyName), KeyType: "RANGE",
			})
		}

		request := svc.CreateTableRequest(&dynamodb.CreateTableInput{
			TableName:             aws.String(newTableName),
			AttributeDefinitions:  attrDefs,
			KeySchema:             keySchema,
			BillingMode:           dynamodb.BillingModePayPerRequest,
			ProvisionedThroughput: db.getDefIops(),
		})

		_, err := request.Send(ctx)
		if err != nil {
			return err
		}

		//noinspection GoUnhandledErrorResult
		svc.WaitUntilTableExists(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(newTableName),
		})

		err = db.ensureTtlIsSet(ctx, svc, newTableName, t.TtlFieldName)
		if err != nil {
			return err
		}

		err = db.ensureGsiIsCreated(ctx, svc, newTableName, t.GSI)
		if err != nil {
			return err
		}
	}

	CLS(ctx).Infof("All tables are ready")
	return nil
}

func (db *DynamoDbSchemer) getDefIops() *dynamodb.ProvisionedThroughput {
	var iops *dynamodb.ProvisionedThroughput
	if db.TestMode {
		iops = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(100),
			WriteCapacityUnits: aws.Int64(100),
		}
	}
	return iops
}

func (db *DynamoDbSchemer) ensureTtlIsSet(ctx context.Context,
	client *dynamodb.Client, tableName string, ttlField string) error {

	if ttlField == "" {
		return nil
	}

	response, err := client.DescribeTimeToLiveRequest(&dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(tableName)}).Send(ctx)
	if err != nil {
		return err
	}

	if response.TimeToLiveDescription == nil ||
		response.TimeToLiveDescription.TimeToLiveStatus == dynamodb.TimeToLiveStatusDisabled {

		CLS(ctx).Infof("Setting TTL field on %s to %s", tableName, ttlField)
		_, err := client.UpdateTimeToLiveRequest(&dynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(tableName),
			TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
				AttributeName: aws.String(ttlField),
				Enabled:       aws.Bool(true),
			},
		}).Send(ctx)
		if err != nil {
			return err
		}
		CLS(ctx).Infof("Updated the TTL field on %s to %s", tableName, ttlField)
	}

	return nil
}

func (db *DynamoDbSchemer) ensureGsiIsCreated(ctx context.Context, client *dynamodb.Client,
	tableName string, gsi map[string]string) error {

	if gsi == nil || len(gsi) == 0 {
		return nil
	}

	CLS(ctx).Infof("Checking the GSI for %s", tableName)

	response, err := client.DescribeTableRequest(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}).Send(ctx)
	if err != nil {
		return err
	}
	existing := make(map[string]int)
	for _, i := range response.DescribeTableOutput.Table.GlobalSecondaryIndexes {
		existing[*i.IndexName] = 1
	}

	var updates []dynamodb.GlobalSecondaryIndexUpdate
	var attrDefs []dynamodb.AttributeDefinition
	for idxName, idxColumn := range gsi {
		if _, ok := existing[idxName]; ok {
			CLS(ctx).Infof("GSI %s exists for %s", idxName, tableName)
			continue
		}

		updates = append(updates, dynamodb.GlobalSecondaryIndexUpdate{
			Create: &dynamodb.CreateGlobalSecondaryIndexAction{
				IndexName: aws.String(idxName),
				KeySchema: []dynamodb.KeySchemaElement{{
					AttributeName: aws.String(idxColumn),
					KeyType:       dynamodb.KeyTypeHash,
				}},
				Projection: &dynamodb.Projection{
					ProjectionType: dynamodb.ProjectionTypeAll,
				},
				ProvisionedThroughput: db.getDefIops(),
			},
		})
		attrDefs = append(attrDefs, dynamodb.AttributeDefinition{
			AttributeName: aws.String(idxColumn), AttributeType: "S"})
	}

	if len(updates) != 0 {
		CLS(ctx).Infof("Creating GSIs for %s", tableName)

		_, err := client.UpdateTableRequest(&dynamodb.UpdateTableInput{
			TableName:                   aws.String(tableName),
			GlobalSecondaryIndexUpdates: updates,
			AttributeDefinitions:        attrDefs,
		}).Send(ctx)
		if err != nil {
			return err
		}
	}

	err = db.waitForGsi(ctx, client, tableName)
	if err != nil {
		return err
	}

	CLS(ctx).Infof("GSI are up-to-date for %s", tableName)
	return nil
}

func (db *DynamoDbSchemer) waitForGsi(ctx context.Context,
	client *dynamodb.Client, tableName string) error {

	// Wait for GSIs to be created
	var hasPendingChanges = true
	for ; hasPendingChanges; {
		response, err := client.DescribeTableRequest(&dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		}).Send(ctx)

		if err != nil {
			return err
		}

		hasPendingChanges = false
		for _, i := range response.DescribeTableOutput.Table.GlobalSecondaryIndexes {
			if i.IndexStatus == dynamodb.IndexStatusCreating {
				hasPendingChanges = true

				// Wait a bit before the retry
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.NewTimer(2 * time.Second).C:
				}
				break
			}
		}
	}

	return nil
}
