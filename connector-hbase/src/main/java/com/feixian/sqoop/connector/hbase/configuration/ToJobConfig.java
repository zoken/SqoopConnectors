/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.feixian.sqoop.connector.hbase.configuration;

import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.validators.NotEmpty;

@ConfigClass
public class ToJobConfig {
	@Input(size = 255, validators = { @Validator(NotEmpty.class) })
	public String tablename;
	@Input(size = 1024, validators = { @Validator(NotEmpty.class) })
	public String columnsMap;
	@Input(size = 1024, validators = { @Validator(NotEmpty.class) })
	public String rowkeyGeneratedWay;
	@Input(size = 1024, validators = { @Validator(NotEmpty.class) })
	public String rowkeyParams;
	@Input(size = 16, validators = { @Validator(NotEmpty.class) })
	public String createTable;
}
