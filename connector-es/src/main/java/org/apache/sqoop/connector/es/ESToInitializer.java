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
package org.apache.sqoop.connector.es;

import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.es.configuration.LinkConfiguration;
import org.apache.sqoop.connector.es.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.utils.ClassUtils;

public class ESToInitializer extends
		Initializer<LinkConfiguration, ToJobConfiguration> {

	private static final Logger LOG = Logger.getLogger(ESToInitializer.class);

	@Override
	public void initialize(InitializerContext context,
			LinkConfiguration linkConfiguration,
			ToJobConfiguration jobConfiguration) {
		LOG.info("Running es Connector initializer. This does nothing except log this message.");
	}

	@Override
	public Set<String> getJars(InitializerContext context,
			LinkConfiguration linkConfiguration,
			ToJobConfiguration toJobConfiguration) {
		Set<String> jars = super.getJars(context, linkConfiguration,
				toJobConfiguration);
		// Jars for Kafka, Scala and Yammer (required by Kafka)
		jars.add(ClassUtils
				.jarForClass("org.elasticsearch.client.transport.TransportClient"));
		jars.add(ClassUtils
				.jarForClass("org.elasticsearch.action.index.IndexResponse"));
		jars.add(ClassUtils
				.jarForClass("org.apache.lucene.util.Version"));
		return jars;
	}

}
