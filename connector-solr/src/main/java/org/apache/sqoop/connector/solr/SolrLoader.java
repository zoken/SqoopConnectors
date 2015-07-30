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
package org.apache.sqoop.connector.solr;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.sqoop.connector.solr.configuration.LinkConfiguration;
import org.apache.sqoop.connector.solr.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;

public class SolrLoader extends Loader<LinkConfiguration, ToJobConfiguration> {
	private static final Logger LOG = Logger.getLogger(SolrLoader.class);
	private int rowWritten = 0;

	@Override
	public void load(LoaderContext context,
			LinkConfiguration linkConfiguration,
			ToJobConfiguration jobConfiguration) throws Exception {
		Object[] record = null;
		String[] solrcolumns = jobConfiguration.toJobConfig.columns.split(",");
		HttpSolrClient client = new HttpSolrClient(
				linkConfiguration.linkConfig.solrmasterurl
						+ jobConfiguration.toJobConfig.instance);
		List<SolrInputDocument> sidocs = new ArrayList<SolrInputDocument>();
		while ((record = context.getDataReader().readArrayRecord()) != null) {
			if (solrcolumns.length != record.length) {
				LOG.warn("SolrLoader load record.length = " + record.length
						+ ", but sorlcolumns.length = " + solrcolumns.length
						+ ". quit record.");
				continue;
			} else {
				SolrInputDocument sidoc = new SolrInputDocument();
				for (int i = 0; i < solrcolumns.length; i++) {
					sidoc.addField(solrcolumns[i], record[i]);
				}
				sidocs.add(sidoc);
			}
		}
		LOG.debug("load docs " + sidocs.size());
		client.add(sidocs);
		client.commit();
		rowWritten = sidocs.size();
		LOG.debug("upload docs successfully");
		client.close();
	}

	@Override
	public long getRowsWritten() {
		// TODO Auto-generated method stub
		return rowWritten;
	}
}
