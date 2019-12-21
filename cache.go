package localcache

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/patrickmn/go-cache"

	_ "github.com/davecgh/go-spew/spew"
)

func New(p client.ConfigProvider, cfgs ...*aws.Config) dynamodbiface.DynamoDBAPI {
	db := dynamodb.New(p, cfgs...)
	return NewWithDB(db)
}

func NewWithDB(client *dynamodb.DynamoDB) dynamodbiface.DynamoDBAPI {
	return &Cache{
		DynamoDB: client,

		items:     cache.New(5*time.Minute, 10*time.Minute),
		tableDesc: cache.New(cache.NoExpiration, cache.NoExpiration),
	}
}

type Cache struct {
	*dynamodb.DynamoDB
	items     *cache.Cache
	tableDesc *cache.Cache
}

func (c *Cache) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	// spew.Dump(input)
	key := *input.TableName + "$" + key2str(input.Key)
	// key := fmt.Sprintf("%s %v"), *input.TableName, *input.)
	if item, ok := c.items.Get(key); ok {
		log.Print("returning cached", key)
		return &dynamodb.GetItemOutput{
			Item: item.(map[string]*dynamodb.AttributeValue),
		}, nil
	}
	out, err := c.DynamoDB.GetItemWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}
	log.Print("caching", key)
	c.items.Set(key, out.Item, cache.DefaultExpiration)
	return out, err
}

func (c *Cache) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	desc, ok := c.tableDesc.Get(*input.TableName)
	if !ok {
		out, err := c.DynamoDB.DescribeTable(&dynamodb.DescribeTableInput{TableName: input.TableName})
		if err != nil {
			return nil, err
		}
		c.tableDesc.Set(*input.TableName, out, cache.DefaultExpiration)
		log.Println("caching desc", out)
		desc = out
	}
	schema := desc.(*dynamodb.DescribeTableOutput).Table.KeySchema
	key := putKey(*input.TableName, input.Item, schema)

	out, err := c.DynamoDB.PutItemWithContext(ctx, input, opts...)
	if err != nil {
		return out, err
	}
	log.Println("caching put", key)
	c.items.Set(key, input.Item, cache.DefaultExpiration)
	return out, err
}

func putKey(table string, item map[string]*dynamodb.AttributeValue, schema []*dynamodb.KeySchemaElement) string {
	key := table + "$" + *schema[0].AttributeName + ":" + av2str(item[*schema[0].AttributeName])
	if len(schema) > 1 {
		key += "/" + *schema[1].AttributeName + ":" + av2str(item[*schema[1].AttributeName])
	}
	return key
}

func key2str(key map[string]*dynamodb.AttributeValue) string {
	if len(key) == 1 {
		for k, v := range key {
			return k + ":" + av2str(v)
		}
	}
	var a, b string
	for k, v := range key {
		if a == "" {
			a = k + ":" + av2str(v)
		}
		b = k + ":" + av2str(v)
	}
	if a[0] < b[0] {
		return a + "/" + b
	}
	return b + "/" + a
}

func av2str(av *dynamodb.AttributeValue) string {
	switch {
	case av.S != nil:
		return *av.S
	case av.B != nil:
		return string(av.B)
	case av.N != nil:
		return *av.N
	}
	panic("invalid key av")
}
