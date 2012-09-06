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


public class RoadSegmentFunction extends BaseOperation implements Function
  {
  public RoadSegmentFunction( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );
   }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry argument = functionCall.getArguments();
    String[] geo_list = argument.getString( 0 ).split( "\\s" );

    for( int i = 0; i < ( geo_list.length - 1 ); i++ )
      {
      String[] p0 = geo_list[i].split( "," );
      Double lng0 = new Double( p0[0] );
      Double lat0 = new Double( p0[1] );
      Double alt0 = new Double( p0[2] );

      String[] p1 = geo_list[i + 1].split( "," );
      Double lng1 = new Double( p1[0] );
      Double lat1 = new Double( p1[1] );
      Double alt1 = new Double( p1[2] );

      Double lat_mid = ( lat0 + lat1 ) / 2.0;
      Double lng_mid = ( lng0 + lng1 ) / 2.0;

      Double slope = ( lat1 - lat0 ) / ( lng1 - lng0 );
      Double intercept = lat0 - ( slope * lng0 );

      Tuple result = new Tuple();

      result.add( lat0 );
      result.add( lng0 );
      result.add( alt0 );

      result.add( lat1 );
      result.add( lng1 );
      result.add( alt1 );

      result.add( lat_mid );
      result.add( lng_mid );

      result.add( slope );
      result.add( intercept );

      functionCall.getOutputCollector().add( result );
      }
    }
  }
