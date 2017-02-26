/*
 * Copyright 2013 NGDATA nv
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
package com.ngdata.hbaseindexer.indexer;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.IOUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Writes updates (new documents and deletes) to Fusion.
 */
public class FusionDocumentWriter implements SolrInputDocumentWriter {

  private static final Log log = LogFactory.getLog(FusionDocumentWriter.class);

  public static MetricName metricName(Class<?> producerClass, String metric, String indexerName) {
    return new MetricName("Lucidworks", producerClass.getSimpleName(), metric, indexerName);
  }

  private Meter atomicUpdatesReceivedMeter;
  private Meter fusionAddMeter;
  private Meter fusionAddErrorMeter;
  private Meter fusionDocsReceivedMeter;
  private Meter solrAtomicUpdatesMeter;
  private Meter solrAtomicUpdatesErrorMeter;
  private Meter fusionDocsProcessedMeter;

  private Meter indexDeleteMeter;

  protected FusionPipelineClient pipelineClient;
  private String solrProxies;
  protected SolrClient solrProxy;
  private final String deleteByQueryAppendString = "|||*";
  private String strIndexName;

  boolean isKerberos = false;
  String fusionUser;

  private static ReentrantLock lock = new ReentrantLock();

  public FusionDocumentWriter(String indexName, Map<String, String> connectionParams) {
    String fusionEndpoint = connectionParams.get("fusion.pipeline");
    if (fusionEndpoint == null)
      throw new IllegalStateException("The 'fusion.pipeline' parameter is required when using Lucidworks Fusion!");

    String fusionSolrProxy = connectionParams.get("fusion.solrproxy");
    if (fusionSolrProxy == null)
      throw new IllegalStateException("The 'fusion.solrproxy' parameter is required when using Lucidworks Fusion!");

    String fusionUser = connectionParams.get("fusion.user");
    this.fusionUser = fusionUser;
    String fusionPass = connectionParams.get("fusion.pass");
    String fusionRealm = connectionParams.get("fusion.realm");

    log.info("Connecting to Fusion pipeline " + fusionEndpoint + " as " + fusionUser + ", realm=" + fusionRealm);
    try {
      pipelineClient = new FusionPipelineClient(fusionEndpoint, fusionUser, fusionPass, fusionRealm);
    } catch (Exception exc) {
      log.error("Failed to create FusionPipelineClient for " + fusionEndpoint + " due to: " + exc);
      if (exc instanceof RuntimeException) {
        throw (RuntimeException) exc;
      } else {
        throw new RuntimeException(exc);
      }
    }

    String fusionLoginConf = System.getProperty(FusionKrb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
    if (fusionLoginConf != null && !fusionLoginConf.isEmpty()) {
      isKerberos = true;
    } else {
      isKerberos = false;
      try {
        log.info("method:FusionDocumentWriter: Create Solr proxy next; fusionSolrProxy:[" + fusionSolrProxy +
          "], indexName:[" + indexName + "].");
        solrProxy = new LBHttpSolrClient(pipelineClient.getHttpClient(), fusionSolrProxy.split(","));
      } catch (Exception exc) {
        log.error("Failed to create LBHttpSolrClient for " + fusionSolrProxy + " due to: " + exc);
        if (exc instanceof RuntimeException) {
          throw (RuntimeException) exc;
        } else {
          throw new RuntimeException(exc);
        }
      }
    }

    solrProxies = fusionSolrProxy; // just used for logging below

    fusionAddMeter = Metrics.newMeter(metricName(getClass(), "Docs sent to Fusion", indexName),
      "Documents sent to Fusion",
      TimeUnit.SECONDS);

    fusionAddErrorMeter = Metrics.newMeter(metricName(getClass(), "Failed Fusion Docs", indexName),
      "Failed docs sent to Fusion",
      TimeUnit.SECONDS);

    fusionDocsReceivedMeter =
      Metrics.newMeter(metricName(getClass(), "Fusion Docs Received", indexName),
        "Docs received (to be processed for Fusion)",
        TimeUnit.SECONDS);

    fusionDocsProcessedMeter =
      Metrics.newMeter(metricName(getClass(), "Fusion Docs Flattened", indexName),
        "Processed docs to send to Fusion (flattened parent / child docs)",
        TimeUnit.SECONDS);


    atomicUpdatesReceivedMeter =
      Metrics.newMeter(metricName(getClass(), "Atomic Updates Received", indexName),
        "Atomic updates received (before processing)",
        TimeUnit.SECONDS);

    solrAtomicUpdatesMeter =
      Metrics.newMeter(metricName(getClass(), "Atomic Updates Sent", indexName),
        "Atomic updates sent to Solr",
        TimeUnit.SECONDS);

    solrAtomicUpdatesErrorMeter =
      Metrics.newMeter(metricName(getClass(), "Failed Atomic Updates", indexName),
        "Failed atomic updates due to Solr errors",
        TimeUnit.SECONDS);

    indexDeleteMeter =
      Metrics.newMeter(metricName(getClass(), "Index deletes", indexName),
        "Documents deleted from Solr index",
        TimeUnit.SECONDS);
    strIndexName = indexName;
    log.info("Fusion document writer initialized successfully for Fusion end point:[" + fusionEndpoint + "]");
  }

  public void add(int shard, Map<String, SolrInputDocument> inputDocumentMap)
    throws SolrServerException, IOException {
    log.debug("kerberosWrapper [" + Wrapper.ADD + "] this " + this);
    if (isKerberos) {
      kerberosWrapper(Wrapper.ADD, shard, inputDocumentMap, 0, null, null);
      return;
    }
    addImpl(shard, inputDocumentMap);
  }

  public void deleteById(int shard, List<String> idsToDelete) throws SolrServerException, IOException {
    log.debug("kerberosWrapper [" + Wrapper.DELETE_BY_ID + "] this " + this);
    if (isKerberos) {
      kerberosWrapper(Wrapper.DELETE_BY_ID, 0, null, shard, idsToDelete, null);
      return;
    }
    deleteByIdImpl(shard, idsToDelete);
  }

  public void deleteByQuery(String deleteQuery) throws SolrServerException, IOException {
    log.debug("kerberosWrapper [" + Wrapper.DELETE_BY_QUERY + "] this " + this);
    if (isKerberos) {
      kerberosWrapper(Wrapper.DELETE_BY_QUERY, 0, null, 0, null, deleteQuery);
      return;
    }
    deleteByQueryImpl(deleteQuery);
  }

  private enum Wrapper {
    ADD,
    DELETE_BY_ID,
    DELETE_BY_QUERY
  }

  private void kerberosWrapper(Wrapper wrapper,
                              int shardAdd, Map<String, SolrInputDocument> inputDocumentMap,
                              int shardDel, List<String> idsToDelete,
                              String deleteQuery) throws SolrServerException, IOException {
    lock.lock();
    try {
      switch (wrapper) {
        case ADD:
          addImpl(shardAdd, inputDocumentMap);
          break;
        case DELETE_BY_ID:
          deleteByIdImpl(shardDel, idsToDelete);
          break;
        case DELETE_BY_QUERY:
          deleteByQueryImpl(deleteQuery);
          break;
        default:
          throw new IOException("Invalid Method.");
      }
    } finally {
      lock.unlock();
    }
  }

  private void deleteByIdImpl(int shard, List<String> idsToDelete) throws SolrServerException, IOException {
    int len = 15;
    String listLogInfo = (idsToDelete.size() > len) ?
      (idsToDelete.subList(0, len).toString() + " + " + (idsToDelete.size() - len) + " more ...") : idsToDelete.toString();
    log.info("Sending a deleteById '" + idsToDelete + "' to Solr(s) at: " + solrProxies);

    boolean deleteByIdsSucceeded = false;
    try {
      getSolrProxy().deleteById(idsToDelete, 500);
      indexDeleteMeter.mark(idsToDelete.size());
      deleteByIdsSucceeded = true;
      // This statement was inserted for Zendesk ticket 4186. If the delete by IDs succeeds above, we also need to ensure
      // that all children documents that have the id (HBase row ID) in their id followed immediately by the
      // deleteByQueryAppendString followed by any additional characters are also deleted from the index.
      deleteByQuery(idsToDelete, "id", deleteByQueryAppendString);
    } catch (Exception e) {
      log.error("Delete docs by " + (deleteByIdsSucceeded ? "query" : "id") + " failed due to: " + e + "; ids: " +
        idsToDelete + (deleteByIdsSucceeded ? " appended with '" + deleteByQueryAppendString : "") +
        ". Retry deleting individually by id.");
      retryDeletesIndividually(idsToDelete, deleteByIdsSucceeded);
    }
  }

  private void deleteByQueryImpl(String deleteQuery) throws SolrServerException, IOException {
    log.info("Sending a deleteByQuery '" + deleteQuery + "' to Solr(s) at: " + solrProxies);
    try {
      getSolrProxy().deleteByQuery(deleteQuery, 500);
    } catch (Exception e) {
      log.error("Failed to execute deleteByQuery(String deleteQuery): " + deleteQuery + " due to: " + e);
    }
  }

  private void addImpl(int shard, Map<String, SolrInputDocument> inputDocumentMap)
    throws SolrServerException, IOException {
    // shs: Processing added to handle atomic updates to documents that were being passed in via the inputDocumentMap.
    List<SolrInputDocument> inputDocuments;
    try {
      inputDocuments = addAtomicUpdateDocuments(inputDocumentMap.values());
      // Catch the exception so that any remaining documents in the collection will be able to be processed and submitted
      // to the fusion indexing pipeline.
    } catch (Exception exc) {
      log.error("Failed to process atomic updates due to: " + exc, exc);
      throw new RuntimeException(exc);
    }

    // shs: Only process the inputDocuments collection if there are still documents remaining in the collection after
    //      the atomic updates have been processed.
    if (inputDocuments != null && !inputDocuments.isEmpty()) {
      fusionDocsReceivedMeter.mark(inputDocuments.size());

      List<Map<String, Object>> fusionDocs;
      try {
        fusionDocs = toJsonDocs(null, inputDocuments, 0);
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
      int numFusionDocsRcvd = fusionDocs != null ? fusionDocs.size() : 0;
      if (numFusionDocsRcvd > 0) {
        fusionDocsProcessedMeter.mark(numFusionDocsRcvd);
        try {
          pipelineClient.postBatchToPipeline(fusionDocs);
          fusionAddMeter.mark(numFusionDocsRcvd);
        } catch (Exception e) {
          log.warn("FusionPipelineClient failed to process batch of " + numFusionDocsRcvd +
            " docs due to: " + e + "; will re-try each doc individually");
          retryFusionAddsIndividually(fusionDocs);
        }
      }
    }
  }

  /**
   * shs: New method added for handling Atomic Updates.
   * @param inputDocuments This is the list of documents that are atomic update documents.
   * @return inputDocuments (modified: the atomic update documents have been removed from the incoming parameter and
   * only non-atomic update documents remain in that collection). The null value will be returned if all
   * documents from the list of SolrInputDocuments have been removed.
   * @throws SolrServerException
   * @throws IOException
   */
  private List<SolrInputDocument> addAtomicUpdateDocuments(Collection<SolrInputDocument> inputDocuments)
    throws SolrServerException, IOException {
    // Visit each document in the collection and determine if it is a document that is an atomic update request. If it
    // is then add it to the atomicUpdateDocs and remove it from inputDocuments.
    Collection<SolrInputDocument> atomicUpdateDocuments = new ArrayList<SolrInputDocument>();

    List<SolrInputDocument> retain = null; // docs that aren't atomic updates

    Iterator<SolrInputDocument> docIterator = inputDocuments.iterator();
    while (docIterator.hasNext()) {
      boolean documentIsAtomic = false;
      SolrInputDocument doc = docIterator.next();
      for (SolrInputField solrInputField : doc.values()) {
        Object val = solrInputField.getValue();
        // If the type of the field just retrieved from the document is a Map object, then this could be an atomic
        // update document.
        if (val instanceof Map) {
          int entryCount = 0;   // Used only for log messages. If the log message below is removed, this may also be deleted.
          for (Map.Entry<String, Object> entry : ((Map<String, Object>) val).entrySet()) {
            String key = entry.getKey();
            if (key.equals("add") || key.equals("set") ||
              key.equals("remove") || key.equals("removeregex") ||
              key.equals("inc")) {
              // keep track of the time we saw this doc on the hbase side
              Map<String, String> atomicUpdateMap = new HashMap<String, String>();
              atomicUpdateMap.put("set", DateUtil.getThreadLocalDateFormat().format(new Date()));
              doc.addField("_hbasets_tdt", atomicUpdateMap);

              // The atomic update documents should be added to the atomicUpdateDocs...
              atomicUpdateDocuments.add(doc);
              documentIsAtomic = true;
              break; // from the entry for loop
            }
            entryCount++;
          }
        }
        // The document was determined to be an atomic update document, no need to visit the rest of the fields, so
        // break from the solrInputField loop.
        if (documentIsAtomic)
          break;

      } // end for

      if (!documentIsAtomic) {
        if (retain == null)
          retain = new ArrayList<SolrInputDocument>();
        retain.add(doc);
      }

    } // end while more docs

    // The following processing is necessary IFF there are documents in the atomicUpdateDocuments collection, which
    // means that we have documents needing an atomic update.
    if (atomicUpdateDocuments != null && !atomicUpdateDocuments.isEmpty()) {
      // For Fusion document submission, the SolrInputDocument must be converted to JSON; however, since these
      // documents are going directly to Solr and are NOT being submitted to a Fusion indexing pipeline, they do not
      // need to be converted to JSON.
      atomicUpdatesReceivedMeter.mark(atomicUpdateDocuments.size());
      try {
        // Submit atomic update documents to Solr at this point.
        getSolrProxy().add(atomicUpdateDocuments, 500);
        solrAtomicUpdatesMeter.mark(atomicUpdateDocuments.size());
      } catch (Exception e) {
        log.warn("Solr failed to process batch of " + atomicUpdateDocuments.size() +
          " atomic updates due to: " + e + "; will re-try each doc individually");
        retrySolrAtomicUpdatesIndividually(atomicUpdateDocuments);
      }
    }
    // Finally, return any documents remaining in the inputDocuments collection for processing through the Fusion
    // indexing pipeline.
    return retain;
  }

  /**
   * shs: This method was modified to add the parent and docCount parameters. These were added to support the recursion
   * needed to flatten (aka denormalize) the parent/child document to any nesting level the input document may have
   * been nested to.
   * @param parentSolrDoc This is the parent document of the list of childSolrDocs. It is in SolrInputDocument format.
   *                      This parameter will be 'null' when first called. When this method is called recursively,
   *                      it will not be null.
   * @param childSolrDocs This is the list documents that are the children (nested) documents of parentSolrDoc.
   * @param docCount      This is the number of child documents for a parent. It is used as the child ID
   * @return The list documents to be sent to Fusion for indexing formatted as JSON documents.
   * @throws Exception
   */
  private List<Map<String, Object>> toJsonDocs(SolrInputDocument parentSolrDoc, Collection<SolrInputDocument> childSolrDocs, int docCount) throws Exception {
    boolean isDebugEnabled = log.isDebugEnabled();
    if (isDebugEnabled) {
      log.debug("Method:toJsonDocs - Processing SolrInputDocuments: parent:[" + (parentSolrDoc == null ? "null" : parentSolrDoc.toString()) +
        "] with " + childSolrDocs.size() + " child documents.");
    }

    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>(childSolrDocs.size());
    for (SolrInputDocument childSolrDoc : childSolrDocs) {
      if (isDebugEnabled) {
        log.debug("Method:toJsonDocs - Processing SolrInputDocuments: parent:[" + (parentSolrDoc == null ? "null" : parentSolrDoc.toString()) +
          "]; child:[" + childSolrDoc.toString() + "]");
      }
      list.addAll(toJson(parentSolrDoc, childSolrDoc, docCount));
    }
    return list;
  }

  /**
   * shs: This method was modified from its original to add the input parameters 'parent' and 'docCount'. This was done
   * to enable recursion to be used to find all parent/child relationships to any level.
   * @param parent   The parent document for the child document being passed in. Parent may be null if the child being
   *                 passed in is a member of the initial documents submitted.
   * @param child    This is the child document. This document will be examined to determine if there are nested
   *                 child documents in it.
   * @param docCount This is the number of child documents for a parent. It is used as the child ID
   * @return The list of JSON formatted documents, denormalized (aka. flattened) and ready to be send to the
   * Fusion indexing pipeline endpoint.
   * @throws Exception
   */
  private List<Map<String, Object>> toJson(SolrInputDocument parent, SolrInputDocument child, int docCount) throws Exception {
    List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();
    if (child != null) {
      List<SolrInputDocument> childDocs = child.getChildDocuments();
      if (childDocs != null && !childDocs.isEmpty()) {
        // Recursive call back to 'toJsonDocs()'
        docs.addAll(toJsonDocs(child, childDocs, docCount++));
      } else {
        // I'm not certain the increment should be on the docCount here...
        docs.add(doc2json(parent, child, docCount++));
      }
    }
    return docs;
  }

  /**
   * shs: This method was modified from its original to add the input parameters 'parent' and 'docCount'. This was done
   * to enable recursion to be used to find all parent/child relationships to any level. The method will merge the
   * fields in the parent document into the child document and will then convert that merged document into JSON
   * format and return that JSON document to the caller.
   * @param parent   The parent document for the child document being passed in. Parent may be null if the child being
   *                 passed in is a member of the initial documents submitted.
   * @param child    This is the child document. It will have the parent's fields merged into it.
   * @param docCount This is a count of the number of documents that have been added in this processing.
   * @return The merged parent and child documents as a JSON formatted document, in a format acceptable to
   * Fusion.
   */
  private Map<String, Object> doc2json(SolrInputDocument parent, SolrInputDocument child, int docCount) {
    Map<String, Object> json = new HashMap<String, Object>();
    if (child != null) {
      String docId = (String) child.getFieldValue("id");
      if (docId == null) {
        if (parent != null) {
          String parentId = (String) parent.getFieldValue("id");
          docId = parentId + "-" + docCount;
        }
        if (docId == null)
          throw new IllegalStateException("Couldn't resolve the id for document: " + child);
      }
      json.put("id", docId);

      List fields = new ArrayList();
      if (parent != null) {
        if (log.isDebugEnabled())
          log.debug("Method:doc2json - Merging parent and child docs, parent:[" + parent.toString() +
            "]; child[" + child.toString() + "].");

        // have a parent doc ... flatten by adding all parent doc fields to the child with prefix _p_
        for (String f : parent.getFieldNames()) {
          if ("id".equals(f)) {
            fields.add(mapField("_p_id", null /* field name prefix */, parent.getField("id").getFirstValue()));
          } else {
            appendField(parent, f, "_p_", fields);
          }
        }
      }
      for (String f : child.getFieldNames()) {
        if (!"id".equals(f)) { // id already added
          appendField(child, f, null, fields);
        }
      }
      // keep track of the time we saw this doc on the hbase side
      String tdt = DateUtil.getThreadLocalDateFormat().format(new Date());
      fields.add(mapField("_hbasets_tdt", null, tdt));
      if (log.isDebugEnabled())
        log.debug(strIndexName + " Reconcile id = " + docId + " and timestamp = " + tdt);

      json.put("fields", fields);
    } else {
      log.warn("method:doc2json - Input parameter 'child' was null.");
    }
    return json;
  }

  private void appendField(SolrInputDocument doc, String f, String pfx, List fields) {
    SolrInputField field = doc.getField(f);
    int vc = field.getValueCount();
    if (vc <= 0)
      return; // no values to add for this field

    if (vc == 1) {
      Map<String, Object> fieldMap = mapField(f, pfx, field.getFirstValue());
      if (fieldMap != null)
        fields.add(fieldMap);
    } else {
      for (Object val : field.getValues()) {
        Map<String, Object> fieldMap = mapField(f, pfx, val);
        if (fieldMap != null)
          fields.add(fieldMap);
      }
    }
  }

  private Map<String, Object> mapField(String f, String pfx, Object val) {
    Map<String, Object> fieldMap = new HashMap<String, Object>(10);
    String fieldName = (pfx != null) ? pfx + f : f;
    fieldMap.put("name", fieldName);
    fieldMap.put("value", val);
    return fieldMap;
  }

  private void retryFusionAddsIndividually(List<Map<String, Object>> docs) throws SolrServerException, IOException {
    for (Map<String, Object> next : docs) {
      try {
        pipelineClient.postBatchToPipeline(Collections.singletonList(next));
        fusionAddMeter.mark();
      } catch (Exception e) {
        log.error("Failed to index document [" + next.get("id") + "] due to: " + e + "; doc: " + next);
        fusionAddErrorMeter.mark();
      }
    }
  }

  /**
   * shs: This method was added to handle documents that are submitted as atomic updates. It is the final option to
   * get documents into the index if the previous indexing attempt failed when sending the entire colleciton of
   * documents to the Solr proxy. In this case, each document in the collection of documents will be submitted to
   * the Solr proxy, one document at a time.
   * @param docs The SolrInputDocuments to be added, one at a time.
   * @throws SolrServerException
   * @throws IOException
   */
  private void retrySolrAtomicUpdatesIndividually(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    for (SolrInputDocument nextDoc : docs) {
      try {
        getSolrProxy().add(nextDoc);
        solrAtomicUpdatesMeter.mark();
      } catch (Exception e) {
        log.error("Failed to index atomic update document [" + nextDoc.get("id") + "] due to: " + e + "; doc: " + nextDoc +
          "solrProxy:[" + solrProxy.toString() + "]");
        solrAtomicUpdatesErrorMeter.mark();
      }
    }
  }

  /**
   * If deleting the documents using a list of document IDs fails, then retry the deletes while iterating through the
   * list of document IDs to be deleted. As the result of changes for ticket 4186, a delete by ID will also result in
   * a delete by query attemp, where the id is postpended by a specific string. In the situation where there were
   * nested (parent/child) documents submitted for indexing, the documents were denormalized and when this happens
   * the document ID of the denormalized child documents contains the ID of the parent (the HBase row ID) followed by a
   * specificpattern of characters followed by an additional string that uniquely identifies the child document. The
   * delete by query will take the parent ID (HBase row ID) and append to it the specific pattern of characters followed
   * by the wild card character ('*'). This will result in all child documents of the parent being deleted from the index.
   * @param idsToDelete             The ID (HBase row ID) of the documents to be deleted
   * @param retryDeletesByQueryOnly If the delete by IDs succeeded and only the delete by queries failed, this will be
   *                                'true' to indicate that only the delete by query should be retried. If the value is
   *                                'false', the delete by id failed, meaning that the delete by query was never
   *                                attempted; thus, both the delete by id AND the delete by query must be attempted.
   * @throws SolrServerException
   * @throws IOException
   */
  private void retryDeletesIndividually(List<String> idsToDelete, boolean retryDeletesByQueryOnly) throws SolrServerException, IOException {
    for (String idToDelete : idsToDelete) {
      if (!retryDeletesByQueryOnly) {
        try {
          getSolrProxy().deleteById(idToDelete, 500);
          indexDeleteMeter.mark();
        } catch (SolrException e) {
          log.error("Failed to delete document with ID " + idToDelete + " due to: " + e + ". Retry deleting by query as '" +
            idToDelete + deleteByQueryAppendString + "'");
        }
      }
      // Try to delete the document by query so as to remove all child documents from the index.
      try {
        deleteByQuery(idToDelete + deleteByQueryAppendString);
      } catch (SolrException e) {
        log.error("Failed to delete document by query inside method 'retryDeletesIndividually' with ID " + idToDelete +
          deleteByQueryAppendString + " due to: " + e + ".");
      }
    }
  }

  private void deleteByQuery(List<String> idsToDelete, String queryFieldName, String deleteQueryAppendStr) throws SolrServerException, IOException {
    for (String idToDelete : idsToDelete) {
      try {
        deleteByQuery(queryFieldName + ":" + idToDelete + deleteQueryAppendStr);
      } catch (Exception e) {
        log.error("Failed to execute deleteByQuery(List<String> idsToDelete, String deleteQueryAppendStr): " + idToDelete + deleteQueryAppendStr + " due to: " + e);
      }
    }
  }

  private synchronized SolrClient getSolrProxy() {
    if (!isKerberos) {
      return solrProxy;
    }
    //close old proxy
    if (solrProxy != null) {
      IOUtils.closeQuietly(solrProxy);
    }
    solrProxy = new LBHttpSolrClient(FusionKrb5HttpClientConfigurer.createClient(fusionUser), solrProxies.split(","));
    return solrProxy;
  }

  public void close() {
    log.info("shutting down pipeline client and solr proxy");
    try {
      pipelineClient.shutdown();
    } catch (Exception ignore) {
    }

    if (solrProxy != null) {
      solrProxy.shutdown();
      try {
      } catch (Exception ignore) {
      }
    }
  }

}
