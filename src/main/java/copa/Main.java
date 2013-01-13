/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 */

package copa;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.pipe.CoGroup;
import cascading.pipe.Checkpoint;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
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
    String logsPath = args[ 3 ];
    String trapPath = args[ 4 ];
    String tsvPath = args[ 5 ];
    String treePath = args[ 6 ];
    String roadPath = args[ 7 ];
    String parkPath = args[ 8 ];
    String shadePath = args[ 9 ];
    String recoPath = args[ 10 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

    // create taps for sources, sinks, traps
    Tap gisTap = new Hfs( new TextLine( new Fields( "line" ) ), gisPath );
    Tap metaTreeTap = new Hfs( new TextDelimited( true, "\t" ), metaTreePath );
    Tap metaRoadTap = new Hfs( new TextDelimited( true, "\t" ), metaRoadPath );
    Tap logsTap = new Hfs( new TextDelimited( true, "," ), logsPath );
    Tap trapTap = new Hfs( new TextDelimited( true, "\t" ), trapPath );
    Tap tsvTap = new Hfs( new TextDelimited( true, "\t" ), tsvPath );
    Tap treeTap = new Hfs( new TextDelimited( true, "\t" ), treePath );
    Tap roadTap = new Hfs( new TextDelimited( true, "\t" ), roadPath );
    Tap parkTap = new Hfs( new TextDelimited( true, "\t" ), parkPath );
    Tap shadeTap = new Hfs( new TextDelimited( true, "\t" ), shadePath );
    Tap recoTap = new Hfs( new TextDelimited( true, "\t" ), recoPath );

    // specify a regex to split the GIS dump into known fields
    Fields fieldDeclaration = new Fields( "blurb", "misc", "geo", "kind" );
    String regex =  "^\"(.*)\",\"(.*)\",\"(.*)\",\"(.*)\"$";
    int[] gisGroups = { 1, 2, 3, 4 };
    RegexParser parser = new RegexParser( fieldDeclaration, regex, gisGroups );
    Pipe gisPipe = new Each( new Pipe( "gis" ), new Fields( "line" ), parser );

    // checkpoint the cleaned-up GIS data
    Checkpoint tsvCheck = new Checkpoint( "tsv", gisPipe );

    // parse the "park" output
    Pipe parkPipe = new Pipe( "park", tsvCheck );
    regex = "^\\s+Community Type\\:\\s+Park.*$";
    parkPipe = new Each( parkPipe, new Fields( "misc" ), new RegexFilter( regex ) );

    // parse the "tree" output
    Pipe treePipe = new Pipe( "tree", tsvCheck );
    regex = "^\\s+Private\\:\\s+(\\S+)\\s+Tree ID\\:\\s+(\\d+)\\s+.*Situs Number\\:\\s+(\\d+)\\s+Tree Site\\:\\s+(\\d+)\\s+Species\\:\\s+(\\S.*\\S)\\s+Source.*$";
    treePipe = new Each( treePipe, new Fields( "misc" ), new RegexFilter( regex ) );

    Fields treeFields = new Fields( "priv", "tree_id", "situs", "tree_site", "raw_species" );
    int[] treeGroups = { 1, 2, 3, 4, 5 };
    parser = new RegexParser( treeFields, regex, treeGroups );
    treePipe = new Each( treePipe, new Fields( "misc" ), parser, Fields.ALL );

    // scrub "species" as a primary key
    regex = "^([\\w\\s]+).*$";
    int[] speciesGroups = { 1 };
    parser = new RegexParser( new Fields( "scrub_species" ), regex, speciesGroups );
    treePipe = new Each( treePipe, new Fields( "raw_species" ), parser, Fields.ALL );
    String expression = "scrub_species.trim().toLowerCase()";
    ExpressionFunction exprFunc = new ExpressionFunction( new Fields( "tree_species" ), expression, String.class );
    treePipe = new Each( treePipe, new Fields( "scrub_species" ), exprFunc, Fields.ALL );

    // join with tree metadata
    Pipe metaTreePipe = new Pipe( "meta_tree" );
    treePipe = new HashJoin( treePipe, new Fields( "tree_species" ), metaTreePipe, new Fields( "species" ), new InnerJoin() );
    treePipe = new Rename( treePipe, new Fields( "blurb" ), new Fields( "tree_name" ) );

    regex = "^(\\S+),(\\S+),(\\S+)\\s*$";
    int[] gpsGroups = { 1, 2, 3 };
    parser = new RegexParser( new Fields( "tree_lat", "tree_lng", "tree_alt" ), regex, gpsGroups );
    treePipe = new Each( treePipe, new Fields( "geo" ), parser, Fields.ALL );

    // determine a tree geohash
    Fields geohashArguments = new Fields( "tree_lat", "tree_lng" );
    treePipe = new Each( treePipe, geohashArguments, new GeoHashFunction( new Fields( "tree_geohash" ), 6 ), Fields.ALL );

    Fields fieldSelector = new Fields( "tree_name", "priv", "tree_id", "situs", "tree_site", "species", "wikipedia", "calflora", "min_height", "max_height", "tree_lat", "tree_lng", "tree_alt", "tree_geohash" );
    treePipe = new Retain( treePipe, fieldSelector );

    // parse the "road" output
    Pipe roadPipe = new Pipe( "road", tsvCheck );
    regex = "^\\s+Sequence\\:.*\\s+Year Constructed\\:\\s+(\\d+)\\s+Traffic Count\\:\\s+(\\d+)\\s+Traffic Index\\:\\s+(\\w.*\\w)\\s+Traffic Class\\:\\s+(\\w.*\\w)\\s+Traffic Date.*\\s+Paving Length\\:\\s+(\\d+)\\s+Paving Width\\:\\s+(\\d+)\\s+Paving Area\\:\\s+(\\d+)\\s+Surface Type\\:\\s+(\\w.*\\w)\\s+Surface Thickness.*\\s+Bike Lane\\:\\s+(\\w+)\\s+Bus Route\\:\\s+(\\w+)\\s+Truck Route\\:\\s+(\\w+)\\s+Remediation.*$";
    roadPipe = new Each( roadPipe, new Fields( "misc" ), new RegexFilter( regex ) );
    Fields roadFields = new Fields( "year_construct", "traffic_count", "traffic_index", "traffic_class", "paving_length", "paving_width", "paving_area", "surface_type", "bike_lane", "bus_route", "truck_route" );
    int[] roadGroups = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
    parser = new RegexParser( roadFields, regex, roadGroups );
    roadPipe = new Each( roadPipe, new Fields( "misc" ), parser, Fields.ALL );

    // join with road metadata
    Pipe metaRoadPipe = new Pipe( "meta_road" );
    roadPipe = new HashJoin( roadPipe, new Fields( "surface_type" ), metaRoadPipe, new Fields( "pavement_type" ), new InnerJoin() );
    roadPipe = new Rename( roadPipe, new Fields( "blurb" ), new Fields( "road_name" ) );

    // estimate albedo based on the road surface age and pavement type
    Fields albedoArguments = new Fields( "year_construct", "albedo_new", "albedo_worn" );
    roadPipe = new Each( roadPipe, albedoArguments, new AlbedoFunction( new Fields( "albedo" ), 2002 ), Fields.ALL );

    // generate road segments, with midpoint, y=mx+b, and road_geohash for each
    Fields segmentArguments = new Fields( "geo" );
    Fields segmentResults = new Fields( "lat0", "lng0", "alt0", "lat1", "lng1", "alt1", "lat_mid", "lng_mid" );
    roadPipe = new Each( roadPipe, segmentArguments, new RoadSegmentFunction( segmentResults ), Fields.ALL );

    geohashArguments = new Fields( "lat_mid", "lng_mid" );
    roadPipe = new Each( roadPipe, geohashArguments, new GeoHashFunction( new Fields( "road_geohash" ), 6 ), Fields.ALL );

    fieldSelector = new Fields( "road_name", "year_construct", "traffic_count", "traffic_index", "traffic_class", "paving_length", "paving_width", "paving_area", "surface_type", "bike_lane", "bus_route", "truck_route", "albedo", "lat0", "lng0", "alt0", "lat1", "lng1", "alt1", "road_geohash" );
    roadPipe = new Retain( roadPipe, fieldSelector );

    // join the tree and road pipes to estimate shade
    Pipe shadePipe = new Pipe( "shade", roadPipe );
    shadePipe = new CoGroup( shadePipe, new Fields( "road_geohash" ), treePipe, new Fields( "tree_geohash" ), new InnerJoin() );

    // calculate a rough estimate for distance from tree to road, then filter for "< ~1 block"
    Fields treeDistArguments = new Fields( "tree_lat", "tree_lng", "lat0", "lng0", "lat1", "lng1" );
    Fields tree_dist = new Fields( "tree_dist" );
    shadePipe = new Each( shadePipe, treeDistArguments, new TreeDistanceFunction( tree_dist ), Fields.ALL );

    ExpressionFilter distFilter = new ExpressionFilter( "tree_dist > 25.0", Double.class );
    shadePipe = new Each( shadePipe, tree_dist, distFilter );

    // checkpoint this (big) calculation too
    fieldSelector = new Fields( "road_name", "year_construct", "traffic_count", "traffic_index", "traffic_class", "paving_length", "paving_width", "paving_area", "surface_type", "bike_lane", "bus_route", "truck_route", "albedo", "lat0", "lng0", "lat1", "lng1", "tree_name", "priv", "tree_id", "situs", "tree_site", "species", "wikipedia", "calflora", "min_height", "max_height", "tree_lat", "tree_lng", "tree_alt", "tree_dist", "tree_geohash" );
    shadePipe = new Retain( shadePipe, fieldSelector );
    shadePipe = new GroupBy( shadePipe, new Fields( "tree_name" ), new Fields( "tree_dist" ) );

    Checkpoint shadeCheck = new Checkpoint( "shade", shadePipe );

    // determine the geohash for GPS tracks log events
    Pipe logsPipe = new Pipe( "logs" );
    geohashArguments = new Fields( "lat", "lng" );
    logsPipe = new Each( logsPipe, geohashArguments, new GeoHashFunction( new Fields( "gps_geohash" ), 6 ), Fields.ALL );

    // prepare data for recommendations
    // NB: RHS is large given the sample data, but in practice the logs on the LHS could be much larger
    Pipe recoPipe = new Pipe( "reco", logsPipe );
    recoPipe = new CoGroup( recoPipe, new Fields( "gps_geohash" ), shadeCheck, new Fields( "tree_geohash" ), new InnerJoin() );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
     .setName( "copa" )
     .addSource( gisPipe, gisTap )
     .addTrap( gisPipe, trapTap )
     .addCheckpoint( tsvCheck, tsvTap )
     .addTailSink( parkPipe, parkTap )
     .addSource( metaTreePipe, metaTreeTap )
     .addSource( metaRoadPipe, metaRoadTap )
     .addSink( treePipe, treeTap )
     .addSink( roadPipe, roadTap )
     .addCheckpoint( shadeCheck, shadeTap )
     .addSource( logsPipe, logsTap )
     .addTailSink( recoPipe, recoTap )
    ;

    // write a DOT file and run the flow
    Flow copaFlow = flowConnector.connect( flowDef );
    copaFlow.writeDOT( "dot/copa.dot" );
    copaFlow.complete();
    }
  }
