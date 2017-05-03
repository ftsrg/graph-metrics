package converter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import com.google.common.collect.ImmutableList;

public class ConverterMain {
  public static void main(String[] args) throws RDFParseException, RepositoryException, IOException {
    final Repository repository = new SailRepository(new MemoryStore());
    repository.initialize();
    // vf = repository.getValueFactory();
    RepositoryConnection connection = repository.getConnection();

    final List<String> filenames = ImmutableList.of( //
        "social_network_activity_0_0.ttl", //
        "social_network_person_0_0.ttl", //
        "social_network_static_0_0.ttl");
    for (String filename : filenames) {
      final File modelFile = new File("src/main/resources/" + filename);
      connection.add(modelFile, null, RDFFormat.TURTLE);
    }

    // assign a unique id for each resource
    final Map<Value, Long> ids = new HashMap<>();
    long id = 0;
    final RepositoryResult<Statement> typeStatements = connection.getStatements(null, RDF.TYPE, null);
    while (typeStatements.hasNext()) {
      final Statement statement = typeStatements.next();
      final Resource subject = statement.getSubject();
      ids.put(subject, id);
      id++;
    }
    final RepositoryResult<Statement> otherStatements = connection.getStatements(null, null, null);
    while (otherStatements.hasNext()) {
      final Statement statement = otherStatements.next();
      final Value object = statement.getObject();
      if (ids.containsKey(object)) {
        continue;
      }
      ids.put(object, id);
      id++;
    }

    RepositoryResult<Statement> statements = connection.getStatements(null, null, null);
    while (statements.hasNext()) {
      final Statement statement = statements.next();

      final Resource subject = statement.getSubject();
      final IRI predicate = statement.getPredicate();
      final Value object = statement.getObject();

      if (predicate.equals(RDF.TYPE) || object instanceof Literal) {
        continue;
      }

      Long subjectId = ids.get(subject);
      Long objectId = ids.get(object);
      System.out.println(subjectId + ":" + objectId + ":" + predicate.getLocalName());
    }
  }
}
