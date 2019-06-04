package com.floragunn.searchsupport.jobs.config;

import java.text.ParseException;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.floragunn.searchsupport.jobs.cluster.JobDistributor;
import com.google.common.collect.Iterators;

public class IndexJobConfigSource<JobType extends JobConfig> implements Iterable<JobType> {
    private final static Logger log = LogManager.getLogger(IndexJobConfigSource.class);

    private final String indexName;
    private final Client client;
    private final JobConfigFactory<JobType> jobFactory;
    private final JobDistributor jobDistributor;

    public IndexJobConfigSource(String indexName, Client client, JobConfigFactory<JobType> jobFactory, JobDistributor jobDistributor) {
        this.indexName = indexName;
        this.client = client;
        this.jobFactory = jobFactory;
        this.jobDistributor = jobDistributor;
    }

    @Override
    public Iterator<JobType> iterator() {
        Iterator<JobType> result = new IndexJobConfigIterator();

        if (jobDistributor != null) {
            result = Iterators.filter(result, (job) -> this.jobDistributor.isJobSelected(job));
        }

        return result;
    }

    private class IndexJobConfigIterator implements Iterator<JobType> {
        private Iterator<SearchHit> searchHitIterator;
        private SearchRequest searchRequest;
        private SearchResponse searchResponse;
        private SearchHits searchHits;
        private JobType current;
        private boolean done = false;

        @Override
        public boolean hasNext() {
            lazyInit();

            return current != null;
        }

        @Override
        public JobType next() {
            lazyInit();

            JobType result = this.current;

            this.current = null;

            return result;
        }

        private void lazyInit() {
            if (this.done) {
                return;
            }

            if (this.searchRequest == null) {
                try {
                    this.searchRequest = new SearchRequest(indexName);
                    // TODO select only active
                    this.searchResponse = client.search(searchRequest).actionGet();
                    this.searchHits = this.searchResponse.getHits();
                    this.searchHitIterator = this.searchHits.iterator();
                } catch (IndexNotFoundException e) {
                    // TODO settings for index?
                    // TODO really good here? Maybe let REST actions create index and skip this silently?
                    client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet();

                    this.done = true;
                    return;
                }
            }

            while (this.current == null && this.searchHitIterator.hasNext()) {
                SearchHit searchHit = this.searchHitIterator.next();
                try {
                    this.current = jobFactory.createFromBytes(searchHit.getId(), searchHit.getSourceRef(), searchHit.getVersion());
                } catch (ParseException e) {
                    log.error("Error while parsing job configuration " + indexName + "/" + searchHit.getId() + ":\n" + searchHit.getSourceAsString(),
                            e);
                }
            }

            if (this.current == null) {
                this.done = true;
            }

        }

    }

}
