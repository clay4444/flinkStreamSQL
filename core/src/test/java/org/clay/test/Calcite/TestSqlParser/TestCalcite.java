package org.clay.test.Calcite.TestSqlParser;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class TestCalcite {

    public static void main(String[] args) throws Exception {

        // Convert query to SqlNode
        String sql = "select price from transactions";
        Config config = SqlParser.configBuilder().build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode node = parser.parseQuery();

        // Convert SqlNode to RelNode
        /*VolcanoPlanner planner = new VolcanoPlanner();
        RexBuilder rexBuilder = createRexBuilder();
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        SqlToRelConverter converter = new SqlToRelConverter(...);
        RelRoot root = converter.convertQuery(node, false, true);*/
    }
}
