package com.example;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.ArrayList;
import java.util.Iterator;

public class EmaislSecondaryIndexQuery {
    public static void main(String[] args) {
//        secondaryIndexCreateTable();

//        secondayIndexDescribeTable();

//        secondaryIndexQueryWeatherTable();

        secondaryIndexQueryEmailTable();

    }

    private static void secondaryIndexQueryWeatherTable() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("WeatherData");
        Index index = table.getIndex("PrecipIndex");

        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression("#d = :v_date and Precipitation = :v_precip")
                .withNameMap(new NameMap()
                        .with("#d", "Date"))
                .withValueMap(new ValueMap()
                        .withString(":v_date","2013-08-10")
                        .withNumber(":v_precip",0));

        ItemCollection<QueryOutcome> items = index.query(spec);
        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            System.out.println(iter.next().toJSONPretty());
        }
    }

    private static void secondaryIndexQueryEmailTable() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("margaritacrprd-email4");
        Index index = table.getIndex("module-istatus-index");

        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression("#m = :m")
                .withNameMap(new NameMap()
                        .with("#m", "module"))
                .withValueMap(new ValueMap()
                        .withString(":m","content-page"))
                    .withMaxPageSize(1);

        ItemCollection<QueryOutcome> items = index.query(spec);
        int pageNum = 0;
        for (Page<Item, QueryOutcome> page : items.pages()) {

            System.out.println("\nPage: " + ++pageNum);

            // Process each item on the current page
            Iterator<Item> item = page.iterator();
            while (item.hasNext()) {
                System.out.println(item.next().toJSONPretty());
            }
        }
    }

    private static void secondayIndexDescribeTable() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("WeatherData");
        TableDescription tableDesc = table.describe();


        Iterator<GlobalSecondaryIndexDescription> gsiIter = tableDesc.getGlobalSecondaryIndexes().iterator();
        while (gsiIter.hasNext()) {
            GlobalSecondaryIndexDescription gsiDesc = gsiIter.next();
            System.out.println("Info for index "
                    + gsiDesc.getIndexName() + ":");

            Iterator<KeySchemaElement> kseIter = gsiDesc.getKeySchema().iterator();
            while (kseIter.hasNext()) {
                KeySchemaElement kse = kseIter.next();
                System.out.printf("\t%s: %s\n", kse.getAttributeName(), kse.getKeyType());
            }
            Projection projection = gsiDesc.getProjection();
            System.out.println("\tThe projection type is: "
                    + projection.getProjectionType());
            if (projection.getProjectionType().toString().equals("INCLUDE")) {
                System.out.println("\t\tThe non-key projected attributes are: "
                        + projection.getNonKeyAttributes());
            }
        }
    }

    private static void secondaryIndexCreateTable() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);


// Attribute definitions
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();

        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName("Location")
                .withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName("Date")
                .withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName("Precipitation")
                .withAttributeType("N"));

// Table key schema
        ArrayList<KeySchemaElement> tableKeySchema = new ArrayList<KeySchemaElement>();
        tableKeySchema.add(new KeySchemaElement()
                .withAttributeName("Location")
                .withKeyType(KeyType.HASH));  //Partition key
        tableKeySchema.add(new KeySchemaElement()
                .withAttributeName("Date")
                .withKeyType(KeyType.RANGE));  //Sort key

// PrecipIndex
        GlobalSecondaryIndex precipIndex = new GlobalSecondaryIndex()
                .withIndexName("PrecipIndex")
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits((long) 10)
                        .withWriteCapacityUnits((long) 1))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL));

        ArrayList<KeySchemaElement> indexKeySchema = new ArrayList<KeySchemaElement>();

        indexKeySchema.add(new KeySchemaElement()
                .withAttributeName("Date")
                .withKeyType(KeyType.HASH));  //Partition key
        indexKeySchema.add(new KeySchemaElement()
                .withAttributeName("Precipitation")
                .withKeyType(KeyType.RANGE));  //Sort key

        precipIndex.setKeySchema(indexKeySchema);

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName("WeatherData")
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits((long) 5)
                        .withWriteCapacityUnits((long) 1))
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(tableKeySchema)
                .withGlobalSecondaryIndexes(precipIndex);

        Table table = dynamoDB.createTable(createTableRequest);
        System.out.println(table.getDescription());
    }
}
