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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class TreeDistanceFunction extends BaseOperation implements Function
  {
  public TreeDistanceFunction( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );
   }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry argument = functionCall.getArguments();
    double tree_lat = argument.getDouble( 0 );
    double tree_lng = argument.getDouble( 1 );
    double lat0 = argument.getDouble( 2 );
    double lng0 = argument.getDouble( 3 );
    double lat1 = argument.getDouble( 4 );
    double lng1 = argument.getDouble( 5 );

    // approximation in meters, based on a euclidean approach
    // for a better metric, try using a Haversine formula

    //double tree_dist = pointToLineDistance( lat0, lng0, lat1, lng1, tree_lat, tree_lng );
    //tree_dist = Math.min( tree_dist, Math.hypot( tree_lat - lat0, tree_lng - lng0 ) );
    //tree_dist = Math.min( tree_dist, Math.hypot( tree_lat - lat1, tree_lng - lng1 ) );

    double tree_dist = Math.hypot( tree_lat - ( ( lat0 + lat1 ) / 2.0 ), tree_lng - ( ( lng0 + lng1 ) / 2.0 ) );

    Tuple result = new Tuple();
    result.add( tree_dist * 61290.0 );
    functionCall.getOutputCollector().add( result );
    }

  // http://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line
  public double pointToLineDistance( double x0, double y0, double x1, double y1, double x_, double y_ )
    {
    double norm_len = Math.hypot( x1 - x0, y1 - y0 );
    return Math.abs( ( x_ - x0 ) * ( y1 - y0 ) - ( y_ - y0 ) * ( x1 - x0 ) ) / norm_len;
    }
  }
