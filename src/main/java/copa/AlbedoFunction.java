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


public class AlbedoFunction extends BaseOperation implements Function
  {
  protected Integer year_new = 0;

  public AlbedoFunction( Fields fieldDeclaration, Integer year_new )
    {
    super( 1, fieldDeclaration );
    this.year_new = year_new;
   }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry argument = functionCall.getArguments();
    Integer year_construct = argument.getInteger( 0 );
    Double albedo_new = argument.getDouble( 1 );
    Double albedo_worn = argument.getDouble( 2 );

    Double albedo = ( year_construct >= year_new ) ? albedo_new : albedo_worn;

    Tuple result = new Tuple();
    result.add( albedo );
    functionCall.getOutputCollector().add( result );
    }
  }
