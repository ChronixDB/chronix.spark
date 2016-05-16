/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package de.qaware.chronix.spark.api.java.config;

import de.qaware.chronix.storage.solr.ChronixSolrCloudStorage;
import de.qaware.chronix.storage.solr.mock.ChronixSolrCloudStorageMock;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Map;

public class ChronixYAMLConfiguration {

    private String profile;
    private String appName;
    private String solrReferenceQuery;
    private String chronixCollection;

    private Map<String, String> sparkSettings;
    private Map<String, IndividualConfiguration> configurations;

    public String getAppName() {
        return appName;
    }

    public String getSolrReferenceQuery() {
        return solrReferenceQuery;
    }

    public String getChronixCollection() {
        return chronixCollection;
    }

    public Map<String, String> getSparkSettings() {
        return sparkSettings;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setSolrReferenceQuery(String solrReferenceQuery) {
        this.solrReferenceQuery = solrReferenceQuery;
    }

    public void setChronixCollection(String chronixCollection) {
        this.chronixCollection = chronixCollection;
    }

    public void setSparkSettings(Map<String, String> sparkSettings) {
        this.sparkSettings = sparkSettings;
    }

    public void setConfigurations(Map<String, IndividualConfiguration> configurations) {
        this.configurations = configurations;
    }

    public ChronixSolrCloudStorage getStorage() {
        if (getZookeeperHost().isEmpty())
            return new ChronixSolrCloudStorageMock(getTestdataFile());
        return new ChronixSolrCloudStorage();
    }

    public boolean isDistributed() {
        return !getSparkMaster().startsWith("local");
    }

    public String getTestdataFile() {
        return getIndividualConfiguration().testdataFile;
    }

    public String getZookeeperHost() {
        return getIndividualConfiguration().zookeeper;
    }

    public String getSparkMaster() {
        return getIndividualConfiguration().sparkMaster;
    }

    public String[] getJars() {
        return getIndividualConfiguration().jars;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                .append("appName", appName)
                .append("chronixCollection", chronixCollection)
                .append("distributed", isDistributed())
                .append("individualConfiguration", getIndividualConfiguration())
                .append("profile", profile)
                .append("solrReferenceQuery", solrReferenceQuery)
                .append("sparkSettings", sparkSettings)
                .append("storage", getStorage())
                .toString();
    }

    private IndividualConfiguration getIndividualConfiguration() {
        return configurations.get(profile);
    }

    protected static class IndividualConfiguration {

        private String testdataFile;
        private String zookeeper;
        private String sparkMaster;
        private String[] jars;

        public void setTestdataFile(String testdataFile) {
            this.testdataFile = testdataFile;
        }

        public void setZookeeper(String zookeeper) {
            this.zookeeper = zookeeper;
        }

        public void setSparkMaster(String sparkMaster) {
            this.sparkMaster = sparkMaster;
        }

        public void setJars(String[] jars) {
            this.jars = jars;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                    .append("sparkMaster", sparkMaster)
                    .append("zookeeper", zookeeper)
                    .append("testdataFile", testdataFile)
                    .append("jars", jars)
                    .toString();
        }
    }
}
