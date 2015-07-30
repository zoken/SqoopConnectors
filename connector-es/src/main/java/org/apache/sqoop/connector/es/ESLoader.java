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

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.es.configuration.LinkConfiguration;
import org.apache.sqoop.connector.es.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESLoader extends Loader<LinkConfiguration, ToJobConfiguration> {
	private static final Logger LOG = Logger.getLogger(ESLoader.class);
	private int rowWritten = 0;

	@Override
	public void load(LoaderContext context,
			LinkConfiguration linkConfiguration,
			ToJobConfiguration jobConfiguration) throws Exception {
		Object[] record = null;
		String[] columns = jobConfiguration.toJobConfig.columns.split(",");
		@SuppressWarnings("resource")
		TransportClient client = new TransportClient()
				.addTransportAddress(new InetSocketTransportAddress(
						linkConfiguration.linkConfig.esmastername,
						Integer.valueOf(linkConfiguration.linkConfig.esmasterport)));
		while ((record = context.getDataReader().readArrayRecord()) != null) {
			if (columns.length != record.length) {
				LOG.warn("ESLoader load record.length = " + record.length
						+ ", but columns.length = " + columns.length
						+ ". quit record.");
				continue;
			} else {
				Map<String, Object> jsondata = new HashMap<String, Object>();
				for (int i = 0; i < columns.length; i++) {
					jsondata.put(columns[i], record[i]);
				}
				client.prepareIndex(jobConfiguration.toJobConfig.index,
						jobConfiguration.toJobConfig.type).setSource(jsondata)
						.execute().actionGet();
				rowWritten++;
			}
		}
		LOG.debug("upload docs successfully");
	}

	@Override
	public long getRowsWritten() {
		// TODO Auto-generated method stub
		return rowWritten;
	}
}
