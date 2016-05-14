package de.qaware.chronix.spark.api.java.config;

import de.qaware.chronix.spark.api.java.ChronixRDD;
import de.qaware.chronix.spark.api.java.ChronixSparkContext;
import org.apache.logging.log4j.core.config.yaml.YamlConfiguration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.representer.Representer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;


public class ChronixSparkLoader {

    private static ChronixYAMLConfiguration chronixYAMLConfiguration;
    private static ChronixSparkContext chronixSparkContext;

    public ChronixSparkLoader() {
        Representer representer = new Representer();
        representer.getPropertyUtils().setSkipMissingProperties(true);

        Constructor constructor = new Constructor(YamlConfiguration.class);

        TypeDescription typeDescription = new TypeDescription(ChronixYAMLConfiguration.class);
        typeDescription.putMapPropertyType("configurations", Object.class, ChronixYAMLConfiguration.IndividualConfiguration.class);
        constructor.addTypeDescription(typeDescription);

        Yaml yaml = new Yaml(constructor, representer);
        yaml.setBeanAccess(BeanAccess.FIELD);

        InputStream in = this.getClass().getClassLoader().getResourceAsStream("test_config.yml");
        chronixYAMLConfiguration = yaml.loadAs(in, ChronixYAMLConfiguration.class);
    }

    public ChronixYAMLConfiguration getConfig() {
        return chronixYAMLConfiguration;
    }

    public ChronixSparkContext createChronixSparkContext() throws IOException{
        if (chronixSparkContext != null)
            return chronixSparkContext;

        SparkConf sparkConf = new SparkConf()
                .setMaster(chronixYAMLConfiguration.getSparkMaster())
                .setAppName(chronixYAMLConfiguration.getAppName());

        ChronixSparkContext.tuneSparkConf(sparkConf);

        //Set spark values given in yaml config
        for(Map.Entry<String, String> setting : chronixYAMLConfiguration.getSparkSettings().entrySet()){
            sparkConf.set(setting.getKey(), setting.getValue());
        }

        if (chronixYAMLConfiguration.isDistributed()) {
            sparkConf.setJars(chronixYAMLConfiguration.getJars());
        }

        chronixSparkContext = new ChronixSparkContext(new JavaSparkContext(sparkConf));
        return chronixSparkContext;
    }

    public ChronixRDD createChronixRDD(ChronixSparkContext chronixSparkContext) throws IOException, SolrServerException {
        SolrQuery query = new SolrQuery(chronixYAMLConfiguration.getSolrReferenceQuery());
        ChronixRDD rdd = chronixSparkContext.query(query,
                chronixYAMLConfiguration.getZookeeperHost(),
                chronixYAMLConfiguration.getChronixCollection(),
                chronixYAMLConfiguration.getStorage());
        return rdd;
    }
}
