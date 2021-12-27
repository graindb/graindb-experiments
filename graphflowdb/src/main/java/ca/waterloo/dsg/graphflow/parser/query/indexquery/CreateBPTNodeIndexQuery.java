package ca.waterloo.dsg.graphflow.parser.query.indexquery;

import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import lombok.Getter;
import lombok.Setter;

public class CreateBPTNodeIndexQuery extends CreateIndexQuery {

    @Setter @Getter int propertyKey = GraphCatalog.ANY;
}
