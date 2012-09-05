/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package copa;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Checkpoint;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;


public class
  Main
  {
  public static void
  main( String[] args )
    {
    String gisPath = args[ 0 ];
    String metaTreePath = args[ 1 ];
    String metaRoadPath = args[ 2 ];
    String trapPath = args[ 3 ];
    String tsvPath = args[ 4 ];
    String treePath = args[ 5 ];
    String roadPath = args[ 6 ];
    String parkPath = args[ 7 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

    // create taps for sources, sinks, traps
    Tap gisTap = new Hfs( new TextLine( new Fields( "line" ) ), gisPath );
    Tap metaTreeTap = new Hfs( new TextDelimited( true, "\t" ), metaTreePath );
    Tap metaRoadTap = new Hfs( new TextDelimited( true, "\t" ), metaRoadPath );
    Tap trapTap = new Hfs( new TextDelimited( true, "\t" ), trapPath );
    Tap tsvTap = new Hfs( new TextDelimited( true, "\t" ), tsvPath );
    Tap treeTap = new Hfs( new TextDelimited( true, "\t" ), treePath );
    Tap roadTap = new Hfs( new TextDelimited( true, "\t" ), roadPath );
    Tap parkTap = new Hfs( new TextDelimited( true, "\t" ), parkPath );

    // specify a regex to split the GIS dump into known fields
    Fields fieldDeclaration = new Fields( "blurb", "misc", "geo", "kind" );
    String regex =  "^\"(.*)\",\"(.*)\",\"(.*)\",\"(.*)\"$";
    int[] gisGroups = { 1, 2, 3, 4 };
    RegexParser parser = new RegexParser( fieldDeclaration, regex, gisGroups );
    Pipe gisPipe = new Each( new Pipe( "gis" ), new Fields( "line" ), parser );
    Checkpoint tsvCheck = new Checkpoint( "tsv", gisPipe );

    // parse the "tree" output
    Pipe treePipe = new Pipe( "tree", tsvCheck );
    regex = "^\\s+Private\\:\\s+(\\S+)\\s+Tree ID\\:\\s+(\\d+)\\s+.*Situs Number\\:\\s+(\\d+)\\s+Tree Site\\:\\s+(\\d+)\\s+Species\\:\\s+(\\S.*\\S)\\s+Source.*$";
    treePipe = new Each( treePipe, new Fields( "misc" ), new RegexFilter( regex ) );

    Fields treeFields = new Fields( "priv", "tree_id", "situs", "tree_site", "raw_species" );
    int[] treeGroups = { 1, 2, 3, 4, 5 };
    parser = new RegexParser( treeFields, regex, treeGroups );
    treePipe = new Each( treePipe, new Fields( "misc" ), parser, Fields.ALL );

    regex = "^([\\w\\s]+).*$";
    int[] speciesGroups = { 1 };
    parser = new RegexParser( new Fields( "scrub_species" ), regex, speciesGroups );
    treePipe = new Each( treePipe, new Fields( "raw_species" ), parser, Fields.ALL );

    String expression = "scrub_species.trim().toLowerCase()";
    ExpressionFunction exprFunc = new ExpressionFunction( new Fields( "tree_species" ), expression, String.class );
    treePipe = new Each( treePipe, new Fields( "scrub_species" ), exprFunc, Fields.ALL );

    Pipe metaTreePipe = new Pipe( "meta_tree" );
    treePipe = new HashJoin( treePipe, new Fields( "tree_species" ), metaTreePipe, new Fields( "species" ), new InnerJoin() );

    Fields fieldSelector = new Fields( "blurb", "geo", "priv", "tree_id", "situs", "tree_site", "species", "wikipedia", "calflora", "min_height", "max_height" );
    treePipe = new Retain( treePipe, fieldSelector );

    // parse the "road" output
    Pipe roadPipe = new Pipe( "road", tsvCheck );
    regex = "^\\s+Sequence\\:.*\\s+Year Constructed\\:\\s+(\\d+)\\s+Traffic Count\\:\\s+(\\d+)\\s+Traffic Index\\:\\s+(\\w.*\\w)\\s+Traffic Class\\:\\s+(\\w.*\\w)\\s+Traffic Date.*\\s+Paving Length\\:\\s+(\\d+)\\s+Paving Width\\:\\s+(\\d+)\\s+Paving Area\\:\\s+(\\d+)\\s+Surface Type\\:\\s+(\\w.*\\w)\\s+Surface Thickness.*\\s+Bike Lane\\:\\s+(\\w+)\\s+Bus Route\\:\\s+(\\w+)\\s+Truck Route\\:\\s+(\\w+)\\s+Remediation.*$";
    roadPipe = new Each( roadPipe, new Fields( "misc" ), new RegexFilter( regex ) );
    Fields roadFields = new Fields( "year_construct", "traffic_count", "traffic_index", "traffic_class", "paving_length", "paving_width", "paving_area", "surface_type", "bike_lane", "bus_route", "truck_route" );
    int[] roadGroups = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
    parser = new RegexParser( roadFields, regex, roadGroups );
    roadPipe = new Each( roadPipe, new Fields( "misc" ), parser, Fields.ALL );

    Pipe metaRoadPipe = new Pipe( "meta_road" );
    roadPipe = new HashJoin( roadPipe, new Fields( "surface_type" ), metaRoadPipe, new Fields( "pavement_type" ), new InnerJoin() );

    fieldSelector = new Fields( "blurb", "geo", "year_construct", "traffic_count", "traffic_index", "traffic_class", "paving_length", "paving_width", "paving_area", "surface_type", "bike_lane", "bus_route", "truck_route", "albedo_new", "albedo_worn" );
    roadPipe = new Retain( roadPipe, fieldSelector );

    // parse the "park" output
    Pipe parkPipe = new Pipe( "park", tsvCheck );
    regex = "^\\s+Community Type\\:\\s+Park.*$";
    parkPipe = new Each( parkPipe, new Fields( "misc" ), new RegexFilter( regex ) );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
     .setName( "copa" )
     .addSource( gisPipe, gisTap )
     .addTrap( gisPipe, trapTap )
     .addCheckpoint( tsvCheck, tsvTap )
     .addSource( metaTreePipe, metaTreeTap )
     .addSource( metaRoadPipe, metaRoadTap )
     .addTailSink( treePipe, treeTap )
     .addTailSink( roadPipe, roadTap )
     .addTailSink( parkPipe, parkTap );

    // write a DOT file and run the flow
    Flow copaFlow = flowConnector.connect( flowDef );
    copaFlow.writeDOT( "dot/copa.dot" );
    copaFlow.complete();
    }
  }
