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
    Double lat = argument.getDouble( 0 );
    Double lng = argument.getDouble( 1 );
    Double slope = argument.getDouble( 2 );
    Double intercept = argument.getDouble( 3 );

    // based on:
    // http://math.ucsd.edu/~wgarner/math4c/derivations/distance/distptline.htm
    Double tree_dist = Math.abs( lat - ( slope * lng ) - intercept ) / Math.sqrt( Math.pow( slope, 2.0 ) + 1.0 );

    Tuple result = new Tuple();
    result.add( tree_dist );
    functionCall.getOutputCollector().add( result );
    }
  }
