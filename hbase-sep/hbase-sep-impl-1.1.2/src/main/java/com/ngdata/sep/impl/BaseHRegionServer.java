/*
 * Copyright 2012 NGDATA nv
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
package com.ngdata.sep.impl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateConfigurationResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WarmupRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.GetSpaceQuotaEnforcementsResponse;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.GetSpaceQuotaEnforcementsRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 *
 */
public class BaseHRegionServer implements AdminProtos.AdminService.BlockingInterface, Server, org.apache.hadoop.hbase.ipc.PriorityFunction {

  @Override
  public AdminProtos.GetRegionInfoResponse getRegionInfo(RpcController rpcController, AdminProtos.GetRegionInfoRequest getRegionInfoRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.GetStoreFileResponse getStoreFile(RpcController rpcController, AdminProtos.GetStoreFileRequest getStoreFileRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.GetOnlineRegionResponse getOnlineRegion(RpcController rpcController, AdminProtos.GetOnlineRegionRequest getOnlineRegionRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.OpenRegionResponse openRegion(RpcController rpcController, AdminProtos.OpenRegionRequest openRegionRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.CloseRegionResponse closeRegion(RpcController rpcController, AdminProtos.CloseRegionRequest closeRegionRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller, AdminProtos.UpdateFavoredNodesRequest request) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.FlushRegionResponse flushRegion(RpcController rpcController, AdminProtos.FlushRegionRequest flushRegionRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.SplitRegionResponse splitRegion(RpcController rpcController, AdminProtos.SplitRegionRequest splitRegionRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.CompactRegionResponse compactRegion(RpcController rpcController, AdminProtos.CompactRegionRequest compactRegionRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.MergeRegionsResponse mergeRegions(RpcController rpcController, AdminProtos.MergeRegionsRequest mergeRegionsRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(RpcController rpcController, AdminProtos.ReplicateWALEntryRequest replicateWALEntryRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  
  @Override
  public AdminProtos.ReplicateWALEntryResponse replay(final RpcController controller, final AdminProtos.ReplicateWALEntryRequest request) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }  

  @Override
  public AdminProtos.RollWALWriterResponse rollWALWriter(RpcController rpcController, AdminProtos.RollWALWriterRequest rollWALWriterRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.GetServerInfoResponse getServerInfo(RpcController rpcController, AdminProtos.GetServerInfoRequest getServerInfoRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.StopServerResponse stopServer(RpcController rpcController, AdminProtos.StopServerRequest stopServerRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ServerName getServerName() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void abort(String s, Throwable throwable) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isAborted() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void stop(String s) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isStopped() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getPriority(org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader header, com.google.protobuf.Message param) {
    return org.apache.hadoop.hbase.HConstants.NORMAL_QOS;
  }

  @Override
  public long getDeadline(org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader header, com.google.protobuf.Message param) {
    return 0;
  }

  @Override
  public ClusterConnection getConnection() {
	  throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public MetaTableLocator getMetaTableLocator() {
	  throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
	  throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ChoreService getChoreService() {
	  throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public UpdateConfigurationResponse updateConfiguration(
		RpcController controller, UpdateConfigurationRequest request)
		throws ServiceException {
	throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public WarmupRegionResponse warmupRegion(RpcController controller,
		WarmupRegionRequest request) throws ServiceException {
	throw new UnsupportedOperationException("Not implemented");
  }

 @Override
 public GetSpaceQuotaEnforcementsResponse getSpaceQuotaEnforcements(
          RpcController controller,
          GetSpaceQuotaEnforcementsRequest request)
          throws ServiceException {
       throw new UnsupportedOperationException("Not implemented");
 }

 @Override
 public GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(
          RpcController controller,
          GetSpaceQuotaSnapshotsRequest request)
          throws ServiceException {
       throw new UnsupportedOperationException("Not implemented");
 }

}
