# Examples

## Using the project aggregate to flatten deep nested objects

Document structure: 
> {_id: <some value>, name: "Document", section: {keywords: {keyword: ["word1", "word2", "word3"]}}}

Create a document to select what fields to include in the returned query:
```
// Replace these values with the fields you want to include.
// The $ symbol is important when specifying where to find the values
Document projectFields = new Document("name", "$name").append("keywords", "$section.keywords.keyword");
Bson project = new Document("$project", projectFields);
```

Add the newly created filter into a list of Bson element:
```
List<Bson> filters = new ArrayList<>(Arrays.asList(project));
```

Use the aggregate(String, List<Bson> method of the mongo driver:
```
AggregateIterable<Document> results = mongo.aggregate(<collection name>, filters);
```

