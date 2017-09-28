package ca.on.gov.edu.mongodb;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;

public class MongoDriver {
	
	private MongoClient client;
	private MongoDatabase db;
	private DateFormat dateFormat = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
	private Date dateObject = new Date();
	private String date = dateFormat.format(dateObject);
	private String INFO = "INFO";
	private String WARNING = "WARNING";
	private String ERROR = "ERROR";
	private boolean logging = true;
	private List<Bson> aggregateFilters;
	private String aggregateCollection;
	
	/**
	 * Creates an instance of the mongo database for the user to interact with.
	 * 
	 * @param database The name of the database to connect to, as a String
	 */
	public MongoDriver(String database) {
		// tone down the crazy logging mongo keeps trying to do
		java.util.logging.Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);
		this.client = new MongoClient();
		this.db = client.getDatabase(database);
		this.aggregateFilters = new ArrayList<>();
		this.aggregateCollection = "DEFAULT";
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Connected to MongoDB database '%s'", INFO, date, database));
		}
	}
	
	/**
	 * Creates an instance of the mongo database for the user to interact with.
	 * 
	 * @param database The name of the database to connect to, as a String
	 */
	public MongoDriver(String database, MongoClientURI uri) {
		// tone down the crazy logging mongo keeps trying to do
		java.util.logging.Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);
		this.client = new MongoClient(uri);
		this.db = client.getDatabase(database);
		this.aggregateFilters = new ArrayList<>();
		this.aggregateCollection = "DEFAULT";
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Connected to MongoDB database '%s'", INFO, date, database));
		}
	}
	
	/**
	 * Creates an instance of the mongo database for the user to interact with.
	 * 
	 * @param database The name of the database to connect to, as a String
	 */
	public MongoDriver(String database, String hostname, int port) {
		// tone down the crazy logging mongo keeps trying to do
		java.util.logging.Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);
		this.client = new MongoClient(hostname, port);
		this.db = client.getDatabase(database);
		this.aggregateFilters = new ArrayList<>();
		this.aggregateCollection = "DEFAULT";
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Connected to MongoDB database '%s'", INFO, date, database));
		}
	}
	
	/**
	 * Creates an instance of the mongo database for the user to interact with.
	 * 
	 * @param database The name of the database to connect to, as a String
	 */
	public MongoDriver(String database, String hostname, int port, boolean logging) {
		// tone down the crazy logging mongo keeps trying to do
		java.util.logging.Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);
		this.client = new MongoClient(hostname, port);
		this.db = client.getDatabase(database);
		this.aggregateFilters = new ArrayList<>();
		this.aggregateCollection = "DEFAULT";
		this.logging = logging;
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Connected to MongoDB database '%s'", INFO, date, database));
		}
	}
	
	/**
	 * Creates an instance of the mongo database for the user to interact with.
	 * 
	 * @param database The name of the database to connect to, as a String
	 */
	public MongoDriver(String database, MongoClientURI uri, boolean logging) {
		// tone down the crazy logging mongo keeps trying to do
		java.util.logging.Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);
		this.client = new MongoClient(uri);
		this.db = client.getDatabase(database);
		this.aggregateFilters = new ArrayList<>();
		this.aggregateCollection = "DEFAULT";
		this.logging = logging;
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Connected to MongoDB database '%s'", INFO, date, database));
		}
	}
	
	/**
	 * Creates an instance of the mongo database for the user to interact with.
	 * 
	 * @param database The name of the database to connect to, as a String
	 */
	public MongoDriver(String database, boolean logging) {
		// tone down the crazy logging mongo keeps trying to do
		java.util.logging.Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);
		this.client = new MongoClient();
		this.db = client.getDatabase(database);
		this.aggregateFilters = new ArrayList<>();
		this.aggregateCollection = "DEFAULT";
		this.logging = logging;
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Connected to MongoDB database '%s'", INFO, date, database));
		}
	}	
	
	public String getDatabaseName() {
		return this.db.getName();
	}
	
	/**
	 * Creates the collections specified by the collections parameter without any specific options.
	 * 
	 * @param collections A list of strings containing the names of the collections to be created.
	 */
	public void createCollections(List<String> collections) {
		for (String collection : collections) {
			if (!collectionExists(collection)) {
				createCollection(collection);
			}
		}
	}
	
	/**
	 * Creates a collection with a given name.
	 * 
	 * @param name Name of the collection, as a String
	 */
	public void createCollection(String name) {
		db.createCollection(name);
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Created collection '%s'", INFO, date, name));
		}
	}
	
	/**
	 * Creates a collection with a specific set of options.
	 * 
	 * @param name name of the collection, as a String
	 * @param options options to be applied to the collection (CreateCollectionOptions)
	 */
	public void createCollection(String name, CreateCollectionOptions options) {
		db.createCollection(name, options);
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Created collection '%s' with specified options", INFO, date, name));
		}
	}
	
	/**
	 * Return true if the given collection exists in the database, false otherwise.
	 * 
	 * @param collectionName Name of a collection, as a string
	 * @return True or False
	 */
	public boolean collectionExists(String collectionName) {
		for (String collection : db.listCollectionNames()) {
			if (collection.equalsIgnoreCase(collectionName)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Returns a list of strings containing the names of all the collections in the
	 * selected database.
	 * 
	 * @return List of strings containing collection names
	 */
	public List<String> getCollectionNames() {
		List<String> collectionNames = new ArrayList<>();
		for (String collection : this.db.listCollectionNames()) {
			collectionNames.add(collection);
		}
		return collectionNames;
	}
	
	/**
	 * Returns a ListCollectionsIterable object containing all the collections in the current database.
	 * 
	 * @return ListCollectionsIterable object
	 */
	public ListCollectionsIterable<Document> getCollections() {
		return this.db.listCollections();
	}
	
	/**
	 * Returns an iterable of all documents in the specified collection.
	 * 
	 * @param collection Name of the collection to query, as a String
	 * @return A FindIterable object
	 */
	public FindIterable<Document> find(String collection) {
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Querying database...", INFO, date));
		}
		return this.db.getCollection(collection).find();
	}
	
	/**
	 * Returns an iterable of all documents in the specified collection that match the provided
	 * filter.
	 * 
	 * @param collection The name of the collection to query, as a String
	 * @param filter A Bson filter for the query (e.g Filters.eq("name", "John Doe"))
	 * @return A FindIterable object
	 */
	public FindIterable<Document> find(String collection, Bson filter) {
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Querying database...", INFO, date));
		}
		return this.db.getCollection(collection).find(filter);
	}
	
	/**
	 * Returns an iterable of documents in the specified collection that match the provided
	 * filter, capped by the limit parameter.
	 * 
	 * @param collection The name of the collection to query, as a String
	 * @param limit The max number of documents to return, as an int
	 * @return A FindIterable object
	 */
	public FindIterable<Document> find(String collection, int limit) {
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Querying database (limit of %d documents)...", INFO, date, limit));
		}
		return this.db.getCollection(collection).find().limit(limit);
	}
	
	/**
	 * Returns an iterable of documents in the specified collection that match the provided
	 * filter, capped by the limit parameter.
	 * 
	 * @param collection The name of the collection to query, as a String
	 * @param filter A Bson filter for the query (e.g Filters.eq("name", "John Doe"))
	 * @param limit The max number of documents to return, as an int
	 * @return A FindIterable object
	 */
	public FindIterable<Document> find(String collection, Bson filter, int limit) {
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Querying database (limit of %d documents)...", INFO, date, limit));
		}
		return this.db.getCollection(collection).find(filter).limit(limit);
	}
	
	/**
	 * Inserts a document into the specified collection in the database.
	 * 
	 * @param collection The name of the collection to insert into, as a String
	 * @param doc The document to be insert (Document object)
	 */
	public void insert(String collection, Document doc) {
		try {
			this.db.getCollection(collection).insertOne(doc);
			if (this.logging) {
				System.out.println(String.format("[%s] %s --- Inserted document with ID='%s' into database", INFO, date, doc.get("_id")));
			}
		} catch (MongoWriteException e) {
			if (this.logging) {
				System.out.println(String.format("[%s] %s --- Failed to write to database due to insert command error", ERROR, date));
			}
		} catch (MongoException e) {
			if (this.logging) {
				System.out.println(String.format("[%s] %s --- Failed to write to database due to UNKNOWN", ERROR, date));
			}
		}
	}
	
	/**
	 * Inserts multiple documents into the specified collection in the database.
	 * 
	 * @param collection The name of the collection to insert into, as a String
	 * @param docs A list of Document objects to be inserted
	 */
	public void insert(String collection, List<Document> docs) {
		try {
			this.db.getCollection(collection).insertMany(docs);
			if (this.logging) {
				System.out.println(String.format("[%s] %s --- Inserted %d documents into database", INFO, date, docs.size()));
			}
		} catch (MongoWriteException e) {
			if (this.logging) {
				System.out.println(String.format("[%s] %s --- Failed to write to database due to insert command error", ERROR, date));
			}
		} catch (MongoException e) {
			if (this.logging) {
				System.out.println(String.format("[%s] %s --- Failed to write to database due to UNKNOWN", ERROR, date));
			}
		}
	}
	
	/**
	 * Updates a document in the database with the provided document.
	 * Only updates one document per call.
	 * 
	 * @param collection The name of the collection the document is in, as a String
	 * @param filter A filter to specify which document to update
	 * @param doc The Document object representing the updated document
	 */
	public void update(String collection, Bson filter, Document doc) {
		this.db.getCollection(collection).replaceOne(filter, doc);
	}
	
	/**
	 * Returns the number of documents in the specified collection.
	 * 
	 * @param collection The name of the collection, as a String
	 * @return The number of documents in the collection, as a long
	 */
	public long collectionSize(String collection) {
		return this.db.getCollection(collection).count();
	}
	
	/**
	 * Returns an iterable of documents that contain all the documents that result from the aggregation.
	 * 
	 * @param collection The name of the collection to aggregate on, as a String
	 * @param filters A list of Bson filters containing the aggregation methods
	 * @return An iterable of documents
	 */
	public AggregateIterable<Document> aggregate(String collection, List<Bson> filters) {
		AggregateIterable<Document> documents = null;
		if (this.logging) {
			System.out.println(String.format("[%s] %s --- Performing aggregation...", INFO, date));
			documents = db.getCollection(collection).aggregate(filters);
			System.out.println(String.format("[%s] %s --- Aggregation complete, returning results...", INFO, date));
		} else {
			documents = db.getCollection(collection).aggregate(filters);
		}
		this.aggregateCollection = "DEFAULT";
		this.aggregateFilters.clear();
		return documents;
	}
	
	/**
	 * First step in the builder process to create an aggregate pipeline.
	 * 
	 * @param collectionName The name of the collection to aggregate on, as a String
	 * @return MongoDriver
	 */
	public MongoDriver aggregateBuilder(String collectionName) {
		this.aggregateFilters.clear();
		this.aggregateCollection = collectionName;
		return this;
	}
	
	/**
	 * Add a text search to the aggregate pipeline.
	 * NOTE: For this to work, this must be the very first step after starting the aggregateBuilder.
	 * 
	 * @param search Term to search for, as a String
	 * @return MongoDriver
	 */
	public MongoDriver text(String search) {
		Bson text = new Document("$match",
				new Document("$text",
						new Document("$search", search)));
		this.aggregateFilters.add(text);
		return this;
	}
	
	/**
	 * Add a lookup aggregation to the pipeline.
	 * 
	 * @param fromCollection Collection to join on, as a String
	 * @param localField Local field to match, as a String
	 * @param foreignField Field from the fromCollection to match, as a String
	 * @param as Name for the field that will be created as a result of the lookup, as a String
	 * @return MongoDriver
	 */
	public MongoDriver lookup(String fromCollection, String localField, String foreignField, String as) {
		Bson lookup = new Document("$lookup",
				new Document("from", fromCollection)
							.append("localField", localField)
					.append("foreignField", foreignField)
					.append("as", as));
		this.aggregateFilters.add(lookup);
		return this;
	}
	
	/**
	 * Add an unwind aggregation to the pipeline. Will not unwind non-array objects/fields.
	 * 
	 * @param fieldName Name of the field to flatten, as a String
	 * @return MongoDriver
	 */
	public MongoDriver unwind(String fieldName) {
		Bson unwind = new Document("$unwind", "$" + fieldName);
		this.aggregateFilters.add(unwind);
		return this;
	}
	
	/**
	 * Add a project aggregate to the pipeline.
	 * CURRENTLY DOES NOT WORK.
	 * 
	 * @param fields A Map that maps the fields to be kept to their respective mongo fields where the
	 * values can be found.
	 * @return MongoDriver
	 */
	public MongoDriver project(Map<String, String> fields) {
		Document projectFields = new Document();
		for (Map.Entry<String, String> entry : fields.entrySet()) {
			String field = entry.getKey();
			String value = "$" + entry.getValue();
			projectFields.append(field, value);
		}
		Bson project = new Document("$project", projectFields);
		this.aggregateFilters.add(project);
		return this;
	}
	
	/**
	 * Perform the aggregation that has been build by the aggregateBuilder.
	 * 
	 * @return An iterable containing all the documents that result from the built aggregation
	 */
	public AggregateIterable<Document> aggregate() {
		if (this.logging) {
			if (this.aggregateFilters.size() == 0) {
				System.out.println(String.format("[%s] %s --- No filters have been applied using the aggregate builder, aggregate() will return an empty iterable", WARNING, date));
			} else if (this.aggregateCollection.equals("DEFAULT")) {
				System.out.println(String.format("[%s] %s --- No collection has been set for the aggregate builder, aggregate() will return an empty iterable", WARNING, date));
			}
		}
		return aggregate(this.aggregateCollection, this.aggregateFilters);
	}
}
