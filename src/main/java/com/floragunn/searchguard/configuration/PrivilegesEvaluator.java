/*
 * Copyright 2015 floragunn UG (haftungsbeschränkt)
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
 * 
 */

package com.floragunn.searchguard.configuration;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportRequest;

import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder.SetMultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class PrivilegesEvaluator {

    private static final Set<String> NO_INDICES_SET = Sets.newHashSet("\\",";",",","/","|");
    private static final Set<String> NULL_SET = Sets.newHashSet((String)null);
    private final Set<String> DLSFLS = ImmutableSet.of("_dls_", "_fls_");
    protected final Logger log = LogManager.getLogger(this.getClass());
    private final ClusterService clusterService;
    private final ActionGroupHolder ah;
    private final IndexNameExpressionResolver resolver;
    private final Map<Class<?>, Method> typeCache = Collections.synchronizedMap(new HashMap<Class<?>, Method>(100));
    private final Map<Class<?>, Method> typesCache = Collections.synchronizedMap(new HashMap<Class<?>, Method>(100));
    private final String[] deniedActionPatterns;
    private final AuditLog auditLog;
    private ThreadContext threadContext;
    private final static IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.lenientExpandOpen();
    private final ConfigurationRepository configurationRepository;

    private final String searchguardIndex;
    private PrivilegesInterceptor privilegesInterceptor;
    
    private final boolean enableSnapshotRestorePrivilege;
    private final boolean checkSnapshotRestoreWritePrivileges;
    
    private RoleMappingHolder roleMappingHolder;
    private final TenantHolder tenantHolder;

    public PrivilegesEvaluator(final ClusterService clusterService, final ThreadPool threadPool, final ConfigurationRepository configurationRepository, final ActionGroupHolder ah,
            final IndexNameExpressionResolver resolver, AuditLog auditLog, final Settings settings, final PrivilegesInterceptor privilegesInterceptor) {

        super();
        this.configurationRepository = configurationRepository;
        this.clusterService = clusterService;
        this.ah = ah;
        this.resolver = resolver;
        this.auditLog = auditLog;

        this.threadContext = threadPool.getThreadContext();
        this.searchguardIndex = settings.get(ConfigConstants.SG_CONFIG_INDEX, ConfigConstants.SG_DEFAULT_CONFIG_INDEX);
        this.privilegesInterceptor = privilegesInterceptor;
        this.enableSnapshotRestorePrivilege = settings.getAsBoolean(ConfigConstants.SG_ENABLE_SNAPSHOT_RESTORE_PRIVILEGE,
                ConfigConstants.SG_DEFAULT_ENABLE_SNAPSHOT_RESTORE_PRIVILEGE);
        this.checkSnapshotRestoreWritePrivileges = settings.getAsBoolean(ConfigConstants.SG_CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES,
                ConfigConstants.SG_DEFAULT_CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES);
        
        tenantHolder = new TenantHolder();
        
        this.configurationRepository.subscribeOnChange(ConfigConstants.CONFIGNAME_ROLES, tenantHolder);
        
        this.configurationRepository.subscribeOnChange(ConfigConstants.CONFIGNAME_ROLES_MAPPING, new ConfigurationChangeListener() {
            
            public void onChange(Settings rolesMapping) {
                final RoleMappingHolder tmp = new RoleMappingHolder(rolesMapping);
                roleMappingHolder = tmp;
            }
        });

        /*
        indices:admin/template/delete
        indices:admin/template/get
        indices:admin/template/put
        
        indices:admin/aliases
        indices:admin/aliases/exists
        indices:admin/aliases/get
        indices:admin/analyze
        indices:admin/cache/clear
        -> indices:admin/close
        indices:admin/create
        -> indices:admin/delete
        indices:admin/get
        indices:admin/exists
        indices:admin/flush
        indices:admin/mapping/put
        indices:admin/mappings/fields/get
        indices:admin/mappings/get
        indices:admin/open
        indices:admin/optimize
        indices:admin/refresh
        indices:admin/settings/update
        indices:admin/shards/search_shards
        indices:admin/types/exists
        indices:admin/upgrade
        indices:admin/validate/query
        indices:admin/warmers/delete
        indices:admin/warmers/get
        indices:admin/warmers/put
        */
        
        final List<String> deniedActionPatternsList = new ArrayList<String>();
        deniedActionPatternsList.add("indices:data/write*");
        deniedActionPatternsList.add("indices:admin/close");
        deniedActionPatternsList.add("indices:admin/delete");
        //deniedActionPatternsList.add("indices:admin/settings/update");
        //deniedActionPatternsList.add("indices:admin/upgrade");
        
        deniedActionPatterns = deniedActionPatternsList.toArray(new String[0]);
        
    }
    
    private Settings getRolesSettings() {
        return configurationRepository.getConfiguration(ConfigConstants.CONFIGNAME_ROLES);
    }

    private Settings getConfigSettings() {
        return configurationRepository.getConfiguration(ConfigConstants.CONFIGNAME_CONFIG);
    }
    
    public boolean isInitialized() {
        return roleMappingHolder != null && tenantHolder.initialized && getRolesSettings() != null && getConfigSettings() != null;
    }

    public static class IndexType {

        private String index;
        private String type;

        public IndexType(String index, String type) {
            super();
            this.index = index;
            this.type = type.equals("_all")? "*": type;
        }

        public String getCombinedString() {
            return index+"#"+type;
        }

        public String getIndex() {
            return index;
        }

        public String getType() {
            return type;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((index == null) ? 0 : index.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            IndexType other = (IndexType) obj;
            if (index == null) {
                if (other.index != null)
                    return false;
            } else if (!index.equals(other.index))
                return false;
            if (type == null) {
                if (other.type != null)
                    return false;
            } else if (!type.equals(other.type))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "IndexType [index=" + index + ", type=" + type + "]";
        }
    }

    private static class IndexTypeAction extends IndexType {

        private String action;

        public IndexTypeAction(String index, String type, String action) {
            super(index, type);
            this.action = action;
        }

        @Override
        public String getCombinedString() {
            return super.getCombinedString()+"#"+action;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + ((action == null) ? 0 : action.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            if (!super.equals(obj))
                return false;
            IndexTypeAction other = (IndexTypeAction) obj;
            if (action == null) {
                if (other.action != null)
                    return false;
            } else if (!action.equals(other.action))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "IndexTypeAction [index=" + getIndex() + ", type=" + getType() + ", action=" + action + "]";
        }
    }

    public static class PrivEvalResponse {
        boolean allowed = false;
        Set<String> missingPrivileges = new HashSet<String>();
        Map<String,Set<String>> allowedFlsFields;
        Map<String,Set<String>> queries; 
        
        public boolean isAllowed() {
            return allowed;
        }
        public Set<String> getMissingPrivileges() {
            return new HashSet<String>(missingPrivileges);
        }
        
        public Map<String,Set<String>> getAllowedFlsFields() {
            return allowedFlsFields;
        }
        
        public Map<String,Set<String>> getQueries() {
            return queries;
        }
    }
    
    public PrivEvalResponse evaluate(final User user, String action, final ActionRequest request) {
        
        if (!isInitialized()) {
            throw new ElasticsearchSecurityException("Search Guard is not initialized.");
        }
        
        final PrivEvalResponse presponse = new PrivEvalResponse();
        presponse.missingPrivileges.add(action);
        
        final Settings config = getConfigSettings();
        final Settings roles = getRolesSettings();

        final boolean compositeEnabled = config.getAsBoolean("searchguard.dynamic.composite_enabled", true);
        boolean clusterLevelPermissionRequired = false;
        
        final TransportAddress caller = Objects.requireNonNull((TransportAddress) this.threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS));
        
        if (log.isDebugEnabled()) {
            log.debug("### evaluate permissions for {} on {}", user, clusterService.localNode().getName());
            log.debug("requested {} from {}", action, caller);
        }
        
        if(action.startsWith("cluster:admin/snapshot/restore")) {
            if (enableSnapshotRestorePrivilege) {
                return evaluateSnapshotRestore(user, action, request, caller);
            } else {
                log.warn(action + " is not allowed for a regular user");
                return presponse;
            }
        }
        
        if(action.startsWith("internal:indices/admin/upgrade")) {
            action = "indices:admin/upgrade";
        }


        final ClusterState clusterState = clusterService.state();
        final MetaData metaData = clusterState.metaData();

        final Tuple<Set<String>, Set<String>> requestedResolvedAliasesIndicesTypes = resolve(user, action, request, metaData);
                
        final Set<String> requestedResolvedIndices = Collections.unmodifiableSet(requestedResolvedAliasesIndicesTypes.v1());        
        final Set<IndexType> requestedResolvedIndexTypes;
        
        {
            final Set<IndexType> requestedResolvedIndexTypes0 = new HashSet<IndexType>(requestedResolvedAliasesIndicesTypes.v1().size() * requestedResolvedAliasesIndicesTypes.v2().size());
            
            for(String index: requestedResolvedAliasesIndicesTypes.v1()) {
                for(String type: requestedResolvedAliasesIndicesTypes.v2()) {
                    requestedResolvedIndexTypes0.add(new IndexType(index, type));
                }
            }
            
            requestedResolvedIndexTypes = Collections.unmodifiableSet(requestedResolvedIndexTypes0);
        }
        

        if (log.isDebugEnabled()) {
            log.debug("requested resolved indextypes: {}", requestedResolvedIndexTypes);
        }
        
        if (requestedResolvedIndices.contains(searchguardIndex)
                && WildcardMatcher.matchAny(deniedActionPatterns, action)) {
            auditLog.logSgIndexAttempt(request, action);
            log.warn(action + " for '{}' index is not allowed for a regular user", searchguardIndex);
            return presponse;
        }

        if (requestedResolvedIndices.contains("_all")
                && WildcardMatcher.matchAny(deniedActionPatterns, action)) {
            auditLog.logSgIndexAttempt(request, action);
            log.warn(action + " for '_all' indices is not allowed for a regular user");
            return presponse;
        }
        
        if(requestedResolvedIndices.contains(searchguardIndex) || requestedResolvedIndices.contains("_all")) {
            
            if(request instanceof SearchRequest) {
                ((SearchRequest)request).requestCache(Boolean.FALSE);
                if(log.isDebugEnabled()) {
                    log.debug("Disable search request cache for this request");
                }
            }
            
            if(request instanceof RealtimeRequest) {
                ((RealtimeRequest) request).realtime(Boolean.FALSE);
                if(log.isDebugEnabled()) {
                    log.debug("Disable realtime for this request");
                }
            }
        }
        
        final Set<String> sgRoles = mapSgRoles(user, caller);
       
        if (log.isDebugEnabled()) {
            log.debug("mapped roles for {}: {}", user.getName(), sgRoles);
        }
        
        if(privilegesInterceptor.getClass() != PrivilegesInterceptor.class) {
        
            final Boolean replaceResult = privilegesInterceptor.replaceKibanaIndex(request, action, user, config, requestedResolvedIndices, mapTenants(user, sgRoles));
    
            if (replaceResult == Boolean.TRUE) {
                auditLog.logMissingPrivileges(action, request);
                return presponse;
            }
            
            if (replaceResult == Boolean.FALSE) {
                presponse.allowed = true;
                return presponse;
            }
        }
        
        boolean allowAction = false;
        
        final Map<String,Set<String>> dlsQueries = new HashMap<String, Set<String>>();
        final Map<String,Set<String>> flsFields = new HashMap<String, Set<String>>();

        final Map<String, Set<IndexType>> leftovers = new HashMap<String, Set<IndexType>>();
        
      //--- check inner bulk requests
        final Set<String> additionalPermissionsRequired = new HashSet<>();
        
        if (request instanceof BulkShardRequest) {
            BulkShardRequest bsr = (BulkShardRequest) request;
            for (BulkItemRequest bir : bsr.items()) {
                switch (bir.request().opType()) {
                case CREATE:
                    additionalPermissionsRequired.add(IndexAction.NAME);
                    break;
                case INDEX:
                    additionalPermissionsRequired.add(IndexAction.NAME);
                    break;
                case DELETE:
                    additionalPermissionsRequired.add(DeleteAction.NAME);
                    break;
                case UPDATE:
                    additionalPermissionsRequired.add(UpdateAction.NAME);
                    break;
                }
            }
        }
        
        presponse.missingPrivileges.addAll(additionalPermissionsRequired);

        if(log.isDebugEnabled() && !additionalPermissionsRequired.isEmpty()) {
            log.debug("Additional permissions required: "+additionalPermissionsRequired);
        }
        
        final Set<IndexType> _requestedResolvedIndexTypesGlobal = new HashSet<IndexType>(requestedResolvedIndexTypes);
        
        for (final Iterator<String> iterator = sgRoles.iterator(); iterator.hasNext();) {
            final String sgRole = (String) iterator.next();
            final Settings sgRoleSettings = roles.getByPrefix(sgRole);

            if (sgRoleSettings.names().isEmpty()) {
                
                if (log.isDebugEnabled()) {
                    log.debug("sg_role {} is empty", sgRole);
                }
                
                continue;
            }

            if (log.isDebugEnabled()) {
                log.debug("---------- evaluate sg_role: {}", sgRole);
            }

            if (action.startsWith("cluster:") || action.startsWith("indices:admin/template/delete")
                    || action.startsWith("indices:admin/template/get") || action.startsWith("indices:admin/template/put") 
                || action.startsWith("indices:data/read/scroll")
                //M*
                //be sure to sync the CLUSTER_COMPOSITE_OPS actiongroups
                || (compositeEnabled && action.equals(BulkAction.NAME))
                || (compositeEnabled && action.equals(IndicesAliasesAction.NAME))
                || (compositeEnabled && action.equals(MultiGetAction.NAME))
                //TODO 5mg check multipercolate action and check if there are new m* actions
                //|| (compositeEnabled && action.equals(Multiper.NAME))
                || (compositeEnabled && action.equals(MultiSearchAction.NAME))
                || (compositeEnabled && action.equals(MultiTermVectorsAction.NAME))
                || (compositeEnabled && action.equals("indices:data/read/coordinate-msearch"))
                || (compositeEnabled && action.equals("indices:data/write/reindex"))
                || (compositeEnabled && action.equals("indices:data/read/mpercolate"))
                //|| (compositeEnabled && action.equals(MultiPercolateAction.NAME))
                ) {
                
                final Set<String> resolvedActions = resolveActions(sgRoleSettings.getAsArray(".cluster", new String[0]));
                clusterLevelPermissionRequired = true;
                
                if (log.isDebugEnabled()) {
                    log.debug("  resolved cluster actions:{}", resolvedActions);
                }

                if (WildcardMatcher.matchAny(resolvedActions.toArray(new String[0]), action)) {
                    if (log.isDebugEnabled()) {
                        log.debug("  found a match for '{}' and {}, skip other roles", sgRole, action);
                    }
                    presponse.allowed = true;
                    return presponse;
                } else {
                    //check other roles #108
                    if (log.isDebugEnabled()) {
                        log.debug("  not match found a match for '{}' and {}, check next role", sgRole, action);
                    }
                    continue;
                }
            }

            final Map<String, Settings> permittedAliasesIndices0 = sgRoleSettings.getGroups(".indices");
            final Map<String, Settings> permittedAliasesIndices = new HashMap<String, Settings>(permittedAliasesIndices0.size());
            
            for (String origKey : permittedAliasesIndices0.keySet()) {
                permittedAliasesIndices.put(origKey.replace("${user.name}", user.getName()).replace("${user_name}", user.getName()),
                        permittedAliasesIndices0.get(origKey));
            }

            /*
            sg_role_starfleet:
            indices:
            sf: #<--- is an alias or cindex, can contain wildcards, will be resolved to concrete indices
            # if this contain wildcards we do a wildcard based check
            # if contains no wildcards we resolve this to concrete indices an do a exact check
            #

            ships:  <-- is a type, can contain wildcards
            - READ
            public:
            - 'indices:*'
            students:
            - READ
            alumni:
            - READ
            'admin*':
            - READ
            'pub*':
            '*':
            - READ
             */
            
            final Set<IndexType> _requestedResolvedIndexTypes = new HashSet<IndexType>(requestedResolvedIndexTypes);
            //iterate over all beneath indices:
            permittedAliasesIndices:
            for (final String permittedAliasesIndex : permittedAliasesIndices.keySet()) {

                final String resolvedRole = sgRole;
                final String indexPattern = permittedAliasesIndex;
                
                String dls = roles.get(resolvedRole+".indices."+indexPattern+"._dls_");
                final String[] fls = roles.getAsArray(resolvedRole+".indices."+indexPattern+"._fls_");

                //only when dls and fls != null
                String[] concreteIndices = new String[0];
                
                if((dls != null && dls.length() > 0) || (fls != null && fls.length > 0)) {
                    concreteIndices = resolver.concreteIndexNames(clusterService.state(), DEFAULT_INDICES_OPTIONS,indexPattern);
                }
                
                if(dls != null && dls.length() > 0) {
                    
                    //TODO use UserPropertyReplacer, make it registerable for ldap user
                    dls = dls.replace("${user.name}", user.getName()).replace("${user_name}", user.getName());
                   
                    if(dlsQueries.containsKey(indexPattern)) {
                        dlsQueries.get(indexPattern).add(dls);
                    } else {
                        dlsQueries.put(indexPattern, new HashSet<String>());
                        dlsQueries.get(indexPattern).add(dls);
                    }
                    
                    
                    for (int i = 0; i < concreteIndices.length; i++) {
                        final String ci = concreteIndices[i];
                        if(dlsQueries.containsKey(ci)) {
                            dlsQueries.get(ci).add(dls);
                        } else {
                            dlsQueries.put(ci, new HashSet<String>());
                            dlsQueries.get(ci).add(dls);
                        }
                    }
                    
                                        
                    if (log.isDebugEnabled()) {
                        log.debug("dls query {} for {}", dls, Arrays.toString(concreteIndices));
                    }
                    
                }
                
                if(fls != null && fls.length > 0) {
                    
                    if(flsFields.containsKey(indexPattern)) {
                        flsFields.get(indexPattern).addAll(Sets.newHashSet(fls));
                    } else {
                        flsFields.put(indexPattern, new HashSet<String>());
                        flsFields.get(indexPattern).addAll(Sets.newHashSet(fls));
                    }
                    
                    for (int i = 0; i < concreteIndices.length; i++) {
                        final String ci = concreteIndices[i];
                        if(flsFields.containsKey(ci)) {
                            flsFields.get(ci).addAll(Sets.newHashSet(fls));
                        } else {
                            flsFields.put(ci, new HashSet<String>());
                            flsFields.get(ci).addAll(Sets.newHashSet(fls));
                        }
                    }
                    
                    if (log.isDebugEnabled()) {
                        log.debug("fls fields {} for {}", Sets.newHashSet(fls), Arrays.toString(concreteIndices));
                    }
                    
                }
                
                String[] action0 = null;
                
                if(!additionalPermissionsRequired.isEmpty()) {
                    action0 = additionalPermissionsRequired.toArray(new String[0]);
                } else {
                    action0 = new String[] {action};
                }

                if (WildcardMatcher.containsWildcard(permittedAliasesIndex)) {
                    if (log.isDebugEnabled()) {
                        log.debug("  Try wildcard match for {}", permittedAliasesIndex);
                    }

                    handleIndicesWithWildcard(action0, permittedAliasesIndex, permittedAliasesIndices, requestedResolvedIndexTypes, _requestedResolvedIndexTypes, _requestedResolvedIndexTypesGlobal, requestedResolvedIndices);

                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("  Resolve and match {}", permittedAliasesIndex);
                    }

                    handleIndicesWithoutWildcard(action0, permittedAliasesIndex, permittedAliasesIndices, requestedResolvedIndexTypes, _requestedResolvedIndexTypes, _requestedResolvedIndexTypesGlobal);
                }

                if (log.isDebugEnabled()) {
                    log.debug("For index {} remaining requested local indextype: {}", permittedAliasesIndex, _requestedResolvedIndexTypes);
                    log.debug("For index {} remaining requested global indextype: {}", permittedAliasesIndex, _requestedResolvedIndexTypesGlobal);

                }
                
                if (_requestedResolvedIndexTypes.isEmpty()) { //single role match
                    
                    //check filtered aliases
                    for(String requestAliasOrIndex: requestedResolvedIndices) {      
                        
                        final List<AliasMetaData> filteredAliases = new ArrayList<AliasMetaData>();

                        final IndexMetaData indexMetaData = clusterState.metaData().getIndices().get(requestAliasOrIndex);
                        
                        if(indexMetaData == null) {
                            log.warn("{} does not exist in cluster metadata", requestAliasOrIndex);
                            continue;
                        }
                        
                        final ImmutableOpenMap<String, AliasMetaData> aliases = indexMetaData.getAliases();
                        
                        if(aliases != null && aliases.size() > 0) {
                            
                            if(log.isDebugEnabled()) {
                                log.debug("Aliases for {}: {}", requestAliasOrIndex, aliases);
                            }
                        
                            final Iterator<String> it = aliases.keysIt();
                            while(it.hasNext()) {
                                final String alias = it.next();
                                final AliasMetaData aliasMetaData = aliases.get(alias);
                                
                                if(aliasMetaData != null && aliasMetaData.filteringRequired()) {
                                    filteredAliases.add(aliasMetaData);
                                    if(log.isDebugEnabled()) {
                                        log.debug(alias+" is a filtered alias "+aliasMetaData.getFilter());
                                    }
                                } else {
                                    if(log.isDebugEnabled()) {
                                        log.debug(alias+" is not an alias or does not have a filter");
                                    }
                                }
                            }
                        }

                        if(filteredAliases.size() > 1) {
                            //TODO add queries as dls queries (works only if dls module is installed)
                            final String faMode = config.get("searchguard.dynamic.filtered_alias_mode","warn");
                            
                            if(faMode.equals("warn")) {
                                log.warn("More than one ({}) filtered alias found for same index ({}). This is currently not recommended. Aliases: {}", filteredAliases.size(), requestedResolvedIndices, toString(filteredAliases));
                            } else if (faMode.equals("disallow")) {
                                log.error("More than one ({}) filtered alias found for same index ({}). This is currently not supported. Aliases: {}", filteredAliases.size(), requestedResolvedIndices, toString(filteredAliases));
                                continue permittedAliasesIndices;
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("More than one ({}) filtered alias found for same index ({}). Aliases: {}", filteredAliases.size(), requestedResolvedIndices, toString(filteredAliases));
                                }
                            }
                        }
                    } //end-for
                    
                    if (log.isDebugEnabled()) {
                        log.debug("found a match for '{}.{}', evaluate other roles", sgRole, permittedAliasesIndex);
                    }
                
                    allowAction = true;
                } //end-if
                
            }// end loop permittedAliasesIndices
            
            if(log.isDebugEnabled()) {
                log.debug("Added to leftovers {}=>{}", sgRole, _requestedResolvedIndexTypes);
            }

            leftovers.put(sgRole, _requestedResolvedIndexTypes);
            
        } // end sg role loop
        
        if (!allowAction && config.getAsBoolean("searchguard.dynamic.multi_rolespan_enabled", false)) {
            allowAction = _requestedResolvedIndexTypesGlobal.isEmpty();
        }  
        
        if (!allowAction && log.isInfoEnabled()) {
            
            String[] action0;
            
            if(!additionalPermissionsRequired.isEmpty()) {
                action0 = additionalPermissionsRequired.toArray(new String[0]);
            } else {
                action0 = new String[] {action};
            }
            
            log.info("No {}-level perm match for {} {} [Action [{}]] [RolesChecked {}]", clusterLevelPermissionRequired?"cluster":"index" , user, requestedResolvedIndexTypes, action0, sgRoles);
            log.info("No permissions for {}", leftovers);
        }

        if(!dlsQueries.isEmpty()) {
            
            if(this.threadContext.getHeader(ConfigConstants.SG_DLS_QUERY) != null) {
                if(!dlsQueries.equals((Map<String,Set<String>>) Base64Helper.deserializeObject(this.threadContext.getHeader(ConfigConstants.SG_DLS_QUERY)))) {
                    throw new ElasticsearchSecurityException(ConfigConstants.SG_DLS_QUERY+" does not match (SG 900D)");
                }
            } else {
                this.threadContext.putHeader(ConfigConstants.SG_DLS_QUERY, Base64Helper.serializeObject((Serializable) dlsQueries));
            }
            
            presponse.queries = new HashMap<>(dlsQueries);
            
            if (!requestedResolvedIndices.isEmpty()) {
                for (Iterator<Entry<String, Set<String>>> it = presponse.queries.entrySet().iterator(); it.hasNext();) {
                    Entry<String, Set<String>> entry = it.next();
                    if (!WildcardMatcher.matchAny(entry.getKey(), requestedResolvedIndices, false)) {
                        it.remove();
                    }
                }
            }
        }
        
        if(!flsFields.isEmpty()) {
            
            if(this.threadContext.getHeader(ConfigConstants.SG_FLS_FIELDS) != null) {
                if(!flsFields.equals((Map<String,Set<String>>) Base64Helper.deserializeObject(this.threadContext.getHeader(ConfigConstants.SG_FLS_FIELDS)))) {
                    throw new ElasticsearchSecurityException(ConfigConstants.SG_FLS_FIELDS+" does not match (SG 901D)");
                }
            } else {
                this.threadContext.putHeader(ConfigConstants.SG_FLS_FIELDS, Base64Helper.serializeObject((Serializable)flsFields));
            }
            
            presponse.allowedFlsFields = new HashMap<>(flsFields);
            if (!requestedResolvedIndices.isEmpty()) {
                for (Iterator<Entry<String, Set<String>>> it = presponse.allowedFlsFields.entrySet().iterator(); it.hasNext();) {
                    Entry<String, Set<String>> entry = it.next();
                    if (!WildcardMatcher.matchAny(entry.getKey(), requestedResolvedIndices, false)) {
                        it.remove();
                    }
                }
            }
        }
        
        if(!allowAction 
                && privilegesInterceptor.getClass() != PrivilegesInterceptor.class
                && leftovers.size() > 0) {
            boolean interceptorAllow = privilegesInterceptor.replaceAllowedIndices(request, action, user, config, leftovers);
            presponse.allowed=interceptorAllow;
            return presponse;
        }
        
        presponse.allowed=allowAction;
        return presponse;
    }

    
    //---- end evaluate()
    
    private PrivEvalResponse evaluateSnapshotRestore(final User user, String action, final ActionRequest request, final TransportAddress caller) {
        
        final PrivEvalResponse presponse = new PrivEvalResponse();
        presponse.missingPrivileges.add(action);
        
        if (!(request instanceof RestoreSnapshotRequest)) {
            return presponse;
        }

        final RestoreSnapshotRequest restoreRequest = (RestoreSnapshotRequest) request;

        // Do not allow restore of global state
        if (restoreRequest.includeGlobalState()) {
            auditLog.logSgIndexAttempt(request, action);
            log.warn(action + " with 'include_global_state' enabled is not allowed");
            return presponse;
        }

        // Start resolve for RestoreSnapshotRequest
        final RepositoriesService repositoriesService = Objects.requireNonNull(SearchGuardPlugin.GuiceHolder.getRepositoriesService(), "RepositoriesService not initialized");     
        //hack, because it seems not possible to access RepositoriesService from a non guice class
        final Repository repository = repositoriesService.repository(restoreRequest.repository());
        SnapshotInfo snapshotInfo = null;

        for (final SnapshotId snapshotId : repository.getRepositoryData().getSnapshotIds()) {
            if (snapshotId.getName().equals(restoreRequest.snapshot())) {

                if(log.isDebugEnabled()) {
                    log.debug("snapshot found: {} (UUID: {})", snapshotId.getName(), snapshotId.getUUID());    
                }

                snapshotInfo = repository.getSnapshotInfo(snapshotId);
                break;
            }
        }

        if (snapshotInfo == null) {
            log.warn(action + " for repository '" + restoreRequest.repository() + "', snapshot '" + restoreRequest.snapshot() + "' not found");
            return presponse;
        }

        final List<String> requestedResolvedIndices = SnapshotUtils.filterIndices(snapshotInfo.indices(), restoreRequest.indices(), restoreRequest.indicesOptions());

        if (log.isDebugEnabled()) {
            log.debug("resolved indices for restore to: {}", requestedResolvedIndices.toString());
        }
        // End resolve for RestoreSnapshotRequest

        // Check if the source indices contain the searchguard index
        if (requestedResolvedIndices.contains(searchguardIndex) || requestedResolvedIndices.contains("_all")) {
            auditLog.logSgIndexAttempt(request, action);
            log.warn(action + " for '{}' as source index is not allowed", searchguardIndex);
            return presponse;
        }

        // Check if the renamed destination indices contain the searchguard index
        final List<String> renamedTargetIndices = renamedIndices(restoreRequest, requestedResolvedIndices);
        if (renamedTargetIndices.contains(searchguardIndex) || requestedResolvedIndices.contains("_all")) {
            auditLog.logSgIndexAttempt(request, action);
            log.warn(action + " for '{}' as target index is not allowed", searchguardIndex);
            return presponse;
        }

        // Check if the user has the required role to perform the snapshot restore operation
        final Set<String> sgRoles = mapSgRoles(user, caller);

        if (log.isDebugEnabled()) {
            log.debug("mapped roles: {}", sgRoles);
        }

        boolean allowedActionSnapshotRestore = false;

        final Set<String> renamedTargetIndicesSet = new HashSet<String>(renamedTargetIndices);
        final Set<IndexType> _renamedTargetIndices = new HashSet<IndexType>(renamedTargetIndices.size());
        for(final String index: renamedTargetIndices) {
            for(final String neededAction: ConfigConstants.SG_SNAPSHOT_RESTORE_NEEDED_WRITE_PRIVILEGES) {
                _renamedTargetIndices.add(new IndexTypeAction(index, "*", neededAction));
            }
        }
        
        final Settings roles = getRolesSettings();

        for (final Iterator<String> iterator = sgRoles.iterator(); iterator.hasNext();) {
            final String sgRole = iterator.next();
            final Settings sgRoleSettings = roles.getByPrefix(sgRole);

            if (sgRoleSettings.names().isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("sg_role {} is empty", sgRole);
                }

                continue;
            }

            if (log.isDebugEnabled()) {
                log.debug("---------- evaluate sg_role: {}", sgRole);
            }

            final Set<String> resolvedActions = resolveActions(sgRoleSettings.getAsArray(".cluster", new String[0]));
            if (log.isDebugEnabled()) {
                log.debug("  resolved cluster actions:{}", resolvedActions);
            }

            if (WildcardMatcher.matchAny(resolvedActions.toArray(new String[0]), action)) {
                if (log.isDebugEnabled()) {
                    log.debug("  found a match for '{}' and {}, skip other roles", sgRole, action);
                }
                allowedActionSnapshotRestore = true;
            } else {
                // check other roles #108
                if (log.isDebugEnabled()) {
                    log.debug("  not match found a match for '{}' and {}, check next role", sgRole, action);
                }
            }

            if (checkSnapshotRestoreWritePrivileges) {
                final Map<String, Settings> permittedAliasesIndices0 = sgRoleSettings.getGroups(".indices");
                final Map<String, Settings> permittedAliasesIndices = new HashMap<String, Settings>(permittedAliasesIndices0.size());

                for (final String origKey : permittedAliasesIndices0.keySet()) {
                    permittedAliasesIndices.put(origKey.replace("${user.name}", user.getName()).replace("${user_name}", user.getName()),
                            permittedAliasesIndices0.get(origKey));
                }

                for (final String permittedAliasesIndex : permittedAliasesIndices.keySet()) {
                    if (log.isDebugEnabled()) {
                        log.debug("  Try wildcard match for {}", permittedAliasesIndex);
                    }

                    handleSnapshotRestoreWritePrivileges(ConfigConstants.SG_SNAPSHOT_RESTORE_NEEDED_WRITE_PRIVILEGES, permittedAliasesIndex, permittedAliasesIndices, renamedTargetIndicesSet, _renamedTargetIndices);

                    if (log.isDebugEnabled()) {
                        log.debug("For index {} remaining requested indextypeaction: {}", permittedAliasesIndex, _renamedTargetIndices);
                    }

                }// end loop permittedAliasesIndices
            }
        }

        if (checkSnapshotRestoreWritePrivileges && !_renamedTargetIndices.isEmpty()) {
            allowedActionSnapshotRestore = false;
        }

        if (!allowedActionSnapshotRestore) {
            auditLog.logMissingPrivileges(action, request);
            log.info("No perm match for {} [Action [{}]] [RolesChecked {}]", user, action, sgRoles);
        }
        presponse.allowed = allowedActionSnapshotRestore;
        return presponse;
    }

    private List<String> renamedIndices(final RestoreSnapshotRequest request, final List<String> filteredIndices) {
        final List<String> renamedIndices = new ArrayList<>();
        for (final String index : filteredIndices) {
            String renamedIndex = index;
            if (request.renameReplacement() != null && request.renamePattern() != null) {
                renamedIndex = index.replaceAll(request.renamePattern(), request.renameReplacement());
            }
            renamedIndices.add(renamedIndex);
        }
        return renamedIndices;
    }
    
    public Set<String> mapSgRoles(final User user, final TransportAddress caller) {
        return this.roleMappingHolder.map(user, caller);
    }
    
    /*public Set<String> mapSgRoles(final User user, final TransportAddress caller) {

        final Settings rolesMapping = getRolesMappingSettings();
        
        if(user == null || rolesMapping == null) {
            return Collections.emptySet();
        }

        final Set<String> sgRoles = new TreeSet<String>();
        for (final String roleMap : rolesMapping.names()) {
            
            if (WildcardMatcher.allPatternsMatched(rolesMapping.getAsArray(roleMap+".and_backendroles"), user.getRoles().toArray(new String[0]))) {
                sgRoles.add(roleMap);
                continue;
            }
            
            if (WildcardMatcher.matchAny(rolesMapping.getAsArray(roleMap+".backendroles"), user.getRoles().toArray(new String[0]))) {
                sgRoles.add(roleMap);
                continue;
            }

            if (WildcardMatcher.matchAny(rolesMapping.getAsArray(roleMap+".users"), user.getName())) {
                sgRoles.add(roleMap);
                continue;
            }

            if (caller != null &&  WildcardMatcher.matchAny(rolesMapping.getAsArray(roleMap+".hosts"), caller.getAddress())) {
                sgRoles.add(roleMap);
                continue;
            }

            if (caller != null && WildcardMatcher.matchAny(rolesMapping.getAsArray(roleMap+".hosts"), caller.getHost())) {
                sgRoles.add(roleMap);
                continue;
            }

        }

        return Collections.unmodifiableSet(sgRoles);

    }*/
    
    public Map<String, Boolean> mapTenants(final User user, Set<String> roles) {
        return this.tenantHolder.mapTenants(user, roles);
    }
    
    /*
    public Map<String, Boolean> mapTenants(final User user, final Set<String> sgRoles) {
        if(user == null) {
            return Collections.emptyMap();
        }
        
        final Map<String, Boolean> result = new HashMap<String, Boolean>();
        result.put(user.getName(), true);
        
        for(String sgRole: sgRoles) {
            Settings tenants = getRolesSettings().getByPrefix(sgRole+".tenants.");
            
            if(tenants != null) {
                for(String tenant: tenants.names()) {
                    
                    if(tenant.equals(user.getName())) {
                        continue;
                    }
                    
                    if("RW".equalsIgnoreCase(tenants.get(tenant, "RO"))) {
                        result.put(tenant, true);
                    } else {
                        if(!result.containsKey(tenant)) { //RW outperforms RO
                            result.put(tenant, false);
                        }
                    }
                }
            }
            
        }

        return Collections.unmodifiableMap(result);
    }*/


    private void handleIndicesWithWildcard(final String[] action0, final String permittedAliasesIndex,
            final Map<String, Settings> permittedAliasesIndices, final Set<IndexType> requestedResolvedIndexTypes, 
            final Set<IndexType> _requestedResolvedIndexTypes, 
            final Set<IndexType> _requestedResolvedIndexTypesGlobal, 
            final Set<String> requestedResolvedIndices0) {
        
        List<String> wi = null;
        if (!(wi = WildcardMatcher.getMatchAny(permittedAliasesIndex, requestedResolvedIndices0.toArray(new String[0]))).isEmpty()) {

            if (log.isDebugEnabled()) {
                log.debug("  Wildcard match for {}: {}", permittedAliasesIndex, wi);
            }

            final Set<String> permittedTypes = new HashSet<String>(permittedAliasesIndices.get(permittedAliasesIndex).names());
            permittedTypes.removeAll(DLSFLS);
            
            if (log.isDebugEnabled()) {
                log.debug("  matches for {}, will check now types {}", permittedAliasesIndex, permittedTypes);
            }

            for (final String type : permittedTypes) {
                
                final Set<String> resolvedActions = resolveActions(permittedAliasesIndices.get(permittedAliasesIndex).getAsArray(type));

                if (WildcardMatcher.matchAll(resolvedActions.toArray(new String[0]), action0)) {
                    if (log.isDebugEnabled()) {
                        log.debug("    match requested action {} against {}/{}: {}", action0, permittedAliasesIndex, type, resolvedActions);
                    }

                    for(String it: wi) {                        
                        final IndexType itl = new IndexType(it, type);
                        final boolean removed = wildcardRemoveFromSet(_requestedResolvedIndexTypes, itl);
                        wildcardRemoveFromSet(_requestedResolvedIndexTypesGlobal, itl);
                        
                        if(removed) {
                            log.debug("    removed {}", it+type);
                        } else {
                            log.debug("    no match {} in {}", it+type, _requestedResolvedIndexTypes);
                        }
                    }
                }
                
            }  
        } else {
            if (log.isDebugEnabled()) {
                log.debug("  No wildcard match found for {}", permittedAliasesIndex);
            }

            return;
        }
    }

    private void handleIndicesWithoutWildcard(final String[] action0, final String permittedAliasesIndex,
            final Map<String, Settings> permittedAliasesIndices, final Set<IndexType> requestedResolvedIndexTypes, 
            final Set<IndexType> _requestedResolvedIndexTypes,
            final Set<IndexType> _requestedResolvedIndexTypesGlobal) {

        final Set<String> resolvedPermittedAliasesIndex = new HashSet<String>();
        
        if(!resolver.hasIndexOrAlias(permittedAliasesIndex, clusterService.state())) {
            
            if(log.isDebugEnabled()) {
                log.debug("no permittedAliasesIndex '{}' found for  '{}'", permittedAliasesIndex,  action0);
                
                
                for(String pai: permittedAliasesIndices.keySet()) {
                    Settings paiSettings = permittedAliasesIndices.get(pai);
                    log.debug("permittedAliasesIndices '{}' -> '{}'", permittedAliasesIndices, paiSettings==null?"null":String.valueOf(paiSettings.getAsMap()));
                }
                
                log.debug("requestedResolvedIndexTypes '{}'", requestedResolvedIndexTypes);   
            }
            
            resolvedPermittedAliasesIndex.add(permittedAliasesIndex);

        } else {

            resolvedPermittedAliasesIndex.addAll(Arrays.asList(resolver.concreteIndexNames(
                    clusterService.state(), DEFAULT_INDICES_OPTIONS, permittedAliasesIndex)));
        }

        if (log.isDebugEnabled()) {
            log.debug("  resolved permitted aliases indices for {}: {}", permittedAliasesIndex, resolvedPermittedAliasesIndex);
        }

        //resolvedPermittedAliasesIndex -> resolved indices from role entry n
        final Set<String> permittedTypes = new HashSet<String>(permittedAliasesIndices.get(permittedAliasesIndex).names());
        permittedTypes.removeAll(DLSFLS);
        
        if (log.isDebugEnabled()) {
            log.debug("  matches for {}, will check now types {}", permittedAliasesIndex, permittedTypes);
        }

        for (final String type : permittedTypes) {
            
            final Set<String> resolvedActions = resolveActions(permittedAliasesIndices.get(permittedAliasesIndex).getAsArray(type));

            if (WildcardMatcher.matchAll(resolvedActions.toArray(new String[0]), action0)) {
                if (log.isDebugEnabled()) {
                    log.debug("    match requested action {} against {}/{}: {}", action0, permittedAliasesIndex, type, resolvedActions);
                }

                for(String resolvedPermittedIndex: resolvedPermittedAliasesIndex) {
                    final IndexType itl = new IndexType(resolvedPermittedIndex, type);
                    final boolean removed = wildcardRemoveFromSet(_requestedResolvedIndexTypes, itl);
                    wildcardRemoveFromSet(_requestedResolvedIndexTypesGlobal, itl);
                    
                    if(removed) {
                        log.debug("    removed {}", resolvedPermittedIndex+type);

                    } else {
                        log.debug("    no match {} in {}", resolvedPermittedIndex+type, _requestedResolvedIndexTypes);
                    }
                }
            }
        }
    }

    private void handleSnapshotRestoreWritePrivileges(final Set<String> actions, final String permittedAliasesIndex,
                                              final Map<String, Settings> permittedAliasesIndices, final Set<String> requestedResolvedIndices, final Set<IndexType> requestedResolvedIndices0) {
        List<String> wi = null;
        if (!(wi = WildcardMatcher.getMatchAny(permittedAliasesIndex, requestedResolvedIndices.toArray(new String[0]))).isEmpty()) {

            if (log.isDebugEnabled()) {
                log.debug("  Wildcard match for {}: {}", permittedAliasesIndex, wi);
            }

            // Get actions only for the catch all wildcard type '*'
            final Set<String> resolvedActions = resolveActions(permittedAliasesIndices.get(permittedAliasesIndex).getAsArray("*"));

            if (log.isDebugEnabled()) {
                log.debug("  matches for {}, will check now wildcard type '*'", permittedAliasesIndex);
            }

            List<String> wa = null;
            for (String at : resolvedActions) {
                if (!(wa = WildcardMatcher.getMatchAny(at, actions.toArray(new String[0]))).isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("    match requested actions {} against {}/*: {}", actions, permittedAliasesIndex, resolvedActions);
                    }

                    for (String it : wi) {
                        boolean removed = wildcardRemoveFromSet(requestedResolvedIndices0, new IndexTypeAction(it, "*", at));

                        if (removed) {
                            log.debug("    removed {}", it + '*');
                        } else {
                            log.debug("    no match {} in {}", it + '*', requestedResolvedIndices0);
                        }

                    }
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("  No wildcard match found for {}", permittedAliasesIndex);
            }
        }
    }

    private Tuple<Set<String>, Set<String>> resolve(final User user, final String action, final TransportRequest request,
            final MetaData metaData) {
        
        if(request instanceof PutMappingRequest) {
            
            if (log.isDebugEnabled()) {
                log.debug("PutMappingRequest will be handled in a "
                        + "special way cause they does not return indices via .indices()"
                        + "Instead .getConcreteIndex() must be used");
            }
            
            PutMappingRequest pmr = (PutMappingRequest) request;
            Index concreteIndex = pmr.getConcreteIndex();
            
            if(concreteIndex != null && (pmr.indices() == null || pmr.indices().length == 0)) {
                return new Tuple<Set<String>, Set<String>>(Sets.newHashSet(concreteIndex.getName()), Sets.newHashSet(pmr.type()));
            }
        }


        if (!(request instanceof CompositeIndicesRequest) && !(request instanceof IndicesRequest)) {

            if (log.isDebugEnabled()) {
                log.debug("{} is not an IndicesRequest", request.getClass());
            }

            return new Tuple<Set<String>, Set<String>>(Sets.newHashSet("_all"), Sets.newHashSet("_all"));
        }

        //System.out.println("--------> "+request.getClass().getName());
        
        Set<String> indices = new HashSet<String>();
        Set<String> types = new HashSet<String>();
        
        if (request instanceof CompositeIndicesRequest) {
            
            //System.out.println("    -----> is CompositeIndicesRequest");
            
            if(request instanceof IndicesRequest) {
                
                //System.out.println("    -----> is IndicesRequest");

                final Tuple<Set<String>, Set<String>> t = resolve(user, action, (IndicesRequest) request, metaData);
                indices.addAll(t.v1());
                types.addAll(t.v2());
                
            } else if(request instanceof BulkRequest) {

                //System.out.println("    -----> is BulkRequest");
                
                for(IndicesRequest ar: ((BulkRequest) request).requests()) {
                    final Tuple<Set<String>, Set<String>> t = resolve(user, action, (IndicesRequest) ar, metaData);
                    indices.addAll(t.v1());
                    types.addAll(t.v2());
                }
                
            } else if(request instanceof IndicesRequest) {
                
                //System.out.println("    -----> is IndicesRequest");

                final Tuple<Set<String>, Set<String>> t = resolve(user, action, (IndicesRequest) request, metaData);
                indices.addAll(t.v1());
                types.addAll(t.v2());
                
            } else if(request instanceof MultiGetRequest) {
                
                for(Item item: ((MultiGetRequest) request).getItems()) {
                    final Tuple<Set<String>, Set<String>> t = resolve(user, action, item, metaData);
                    indices.addAll(t.v1());
                    types.addAll(t.v2());
                }
                
            } else if(request instanceof MultiSearchRequest) {
                
                for(ActionRequest ar: ((MultiSearchRequest) request).requests()) {
                    final Tuple<Set<String>, Set<String>> t = resolve(user, action, ar, metaData);
                    indices.addAll(t.v1());
                    types.addAll(t.v2());
                }
                
            } else if(request instanceof MultiTermVectorsRequest) {
                
                for(ActionRequest ar: (Iterable<TermVectorsRequest>) () -> ((MultiTermVectorsRequest) request).iterator()) {
                    final Tuple<Set<String>, Set<String>> t = resolve(user, action, ar, metaData);
                    indices.addAll(t.v1());
                    types.addAll(t.v2());
                }

            } else if(request.getClass().getName().equals("org.elasticsearch.index.reindex.ReindexRequest")) {
                                
                try {
                    Tuple<Set<String>, Set<String>> t = resolve(user, action, (IndicesRequest) request.getClass().getMethod("getDestination").invoke(request), metaData);
                    indices.addAll(t.v1());
                    types.addAll(t.v2());
                    
                    t = resolve(user, action, (IndicesRequest) request.getClass().getMethod("getSearchRequest").invoke(request), metaData);
                    indices.addAll(t.v1());
                    types.addAll(t.v2());
                } catch (Exception e) {
                    log.error("Unable to handle "+request.getClass()+" due to "+e);
                    if(log.isDebugEnabled()) {
                        log.debug(ExceptionsHelper.stackTrace(e));
                    }
                }

            } else if(request.getClass().getName().equals("org.elasticsearch.percolator.MultiPercolateRequest")) {
//=======
//            } else if(request instanceof ReindexRequest) {
//                ReindexRequest reindexRequest = (ReindexRequest) request;
//                Tuple<Set<String>, Set<String>> t = resolveIndicesRequest(user, action, reindexRequest.getDestination(), metaData);
//                indices.addAll(t.v1());
//                types.addAll(t.v2());
//>>>>>>> 9a0da0b... Fix CCS local index handling (SG-681)
                
                try {
                    final List<Object> requests = (List<Object>) request.getClass().getMethod("requests").invoke(request);
                    
                    for(final Object ar: requests) {
                        final Tuple<Set<String>, Set<String>> t = resolve(user, action, (TransportRequest) ar, metaData);
                        indices.addAll(t.v1());
                        types.addAll(t.v2());
                    }
                } catch (Exception e) {
                    log.error("Unable to handle "+request.getClass()+" due to "+e);
                    if(log.isDebugEnabled()) {
                        log.debug(ExceptionsHelper.stackTrace(e));
                    }
                }

            } else {
                log.warn("Can not handle composite request of type '"+request.getClass().getName()+"'for "+action+" here");
            }

        } else {
//<<<<<<< HEAD
//            final Tuple<Set<String>, Set<String>> t = resolve(user, action, (IndicesRequest) request, metaData);
//            indices.addAll(t.v1());
//            types.addAll(t.v2());
//=======
            //ccs goes here
            final Tuple<Set<String>, Set<String>> t = resolve(user, action, (IndicesRequest) request, metaData);
            indices = t.v1();
            types = t.v2();
        }
        
        if(log.isDebugEnabled()) {
            log.debug("pre final indices: {}", indices);
            log.debug("pre final types: {}", types);
        }
        
        if(indices == NO_INDICES_SET) {
            return new Tuple<Set<String>, Set<String>>(Collections.emptySet(), Collections.unmodifiableSet(types));
//>>>>>>> 9a0da0b... Fix CCS local index handling (SG-681)
        }
        
        //for PutIndexTemplateRequest the index does not exists yet typically
        if (IndexNameExpressionResolver.isAllIndices(new ArrayList<String>(indices))) {
            if(log.isDebugEnabled()) {
                log.debug("The following list are '_all' indices: {}", indices);
            }
            
            //fix https://github.com/floragunncom/search-guard/issues/332
            if(!indices.isEmpty()) {
                indices.clear();
                indices.add("_all");
            }
        }

        if (types.isEmpty()) {
            types.add("_all");
        }

        return new Tuple<Set<String>, Set<String>>(Collections.unmodifiableSet(indices), Collections.unmodifiableSet(types));
    }

    private Tuple<Set<String>, Set<String>> resolve(final User user, final String action, final IndicesRequest request,
            final MetaData metaData) {

        if (log.isDebugEnabled()) {
            log.debug("Resolve {} from {}", request.indices(), request.getClass());
        }

        final Class<? extends IndicesRequest> requestClass = request.getClass();
        final Set<String> requestTypes = new HashSet<String>();
        
        Method typeMethod = null;
        if(typeCache.containsKey(requestClass)) {
            typeMethod = typeCache.get(requestClass);
        } else {
            try {
                typeMethod = requestClass.getMethod("type");
                typeCache.put(requestClass, typeMethod);
            } catch (NoSuchMethodException e) {
                typeCache.put(requestClass, null);
            } catch (SecurityException e) {
                log.error("Cannot evaluate type() for {} due to {}", requestClass, e);
            }
            
        }
        
        Method typesMethod = null;
        if(typesCache.containsKey(requestClass)) {
            typesMethod = typesCache.get(requestClass);
        } else {
            try {
                typesMethod = requestClass.getMethod("types");
                typesCache.put(requestClass, typesMethod);
            } catch (NoSuchMethodException e) {
                typesCache.put(requestClass, null);
            } catch (SecurityException e) {
                log.error("Cannot evaluate types() for {} due to {}", requestClass, e);
            }
            
        }
        
        if(typeMethod != null) {
            try {
                String type = (String) typeMethod.invoke(request);
                if(type != null) {
                    requestTypes.add(type);
                }
            } catch (Exception e) {
                log.error("Unable to invoke type() for {} due to {}", e, requestClass, e);
            }
        }
        
        if(typesMethod != null) {
            try {
                final String[] types = (String[]) typesMethod.invoke(request);
                
                if(types != null) {
                    requestTypes.addAll(Arrays.asList(types));
                }
            } catch (Exception e) {
                log.error("Unable to invoke types() for {} due to {}", e, requestClass, e);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("indicesOptions {}", request.indicesOptions());
            log.debug("{} raw indices {}", request.indices()==null?0:request.indices().length, Arrays.toString(request.indices()));
        }

        final Set<String> indices = new HashSet<String>();

        if(request.indices() == null || request.indices().length == 0 || new HashSet<String>(Arrays.asList(request.indices())).equals(NULL_SET)) {
            
            if(log.isDebugEnabled()) {
                log.debug("No indices found in request, assume _all");
            }

            indices.addAll(Arrays.asList(resolver.concreteIndexNames(clusterService.state(), DEFAULT_INDICES_OPTIONS, "*")));
            
        } else {
            
            String[] localIndices = request.indices();
            
            if(request instanceof FieldCapabilitiesRequest || request instanceof SearchRequest) {
                IndicesRequest.Replaceable searchRequest = (IndicesRequest.Replaceable) request;
                final Map<String, OriginalIndices> remoteClusterIndices = SearchGuardPlugin.GuiceHolder.getRemoteClusterService()
                        .groupIndices(searchRequest.indicesOptions(),searchRequest.indices(), idx -> resolver.hasIndexOrAlias(idx, clusterService.state()));
                                
                if (remoteClusterIndices.size() > 1) {
                    // check permissions?

                    final OriginalIndices originalLocalIndices = remoteClusterIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    localIndices = originalLocalIndices.indices();
                    
                    if (log.isDebugEnabled()) {
                        log.debug("remoteClusterIndices keys" + remoteClusterIndices.keySet() + "//remoteClusterIndices "
                                + remoteClusterIndices);
                    }
                    
                    if(localIndices.length == 0) {
                        return new Tuple<Set<String>, Set<String>>(NO_INDICES_SET, requestTypes);
                    }
                }
            }

            try { 
                final String[] dateMathIndices;
                if((dateMathIndices = WildcardMatcher.matches("<*>", localIndices, false)).length > 0) {
                    //date math
                    
                    if(log.isDebugEnabled()) {
                        log.debug("Date math indices detected {} (all: {})", dateMathIndices, localIndices);
                    }
                    
                    for(String dateMathIndex: dateMathIndices) {
                        indices.addAll(Arrays.asList(resolver.resolveDateMathExpression(dateMathIndex)));
                    }
                    
                    if(log.isDebugEnabled()) {
                        log.debug("Resolved date math indices {} to {}", dateMathIndices, indices);
                    }
                    
                    if(localIndices.length > dateMathIndices.length) {
                        for(String nonDateMath: localIndices) {
                            if(!WildcardMatcher.match("<*>", nonDateMath)) {
                                indices.addAll(Arrays.asList(resolver.concreteIndexNames(clusterService.state(), request.indicesOptions(), dateMathIndices)));
                            }
                        }
                        
                        if(log.isDebugEnabled()) {
                            log.debug("Resolved additional non date math indices {} to {}", localIndices, indices);
                        }
                    }

                } else {
                    
                    if(log.isDebugEnabled()) {
                        log.debug("No date math indices found");
                    }
                    
                    indices.addAll(Arrays.asList(resolver.concreteIndexNames(clusterService.state(), request.indicesOptions(), localIndices)));
                    if(log.isDebugEnabled()) {
                        log.debug("Resolved {} to {}", localIndices, indices);
                    }
                }                
            } catch (final Exception e) {
                log.debug("Cannot resolve {} (due to {}) so we use the raw values", Arrays.toString(localIndices), e);
                indices.addAll(Arrays.asList(localIndices));
            }
        }
        
        return new Tuple<Set<String>, Set<String>>(indices, requestTypes);
    }

    private Set<String> resolveActions(final String[] actions) {
        final Set<String> resolvedActions = new HashSet<String>();
        for (int i = 0; i < actions.length; i++) {
            final String string = actions[i];
            final Set<String> groups = ah.getGroupMembers(string);
            if (groups.isEmpty()) {
                resolvedActions.add(string);
            } else {
                resolvedActions.addAll(groups);
            }
        }

        return resolvedActions;
    }
    
    private boolean wildcardRemoveFromSet(Set<IndexType> set, IndexType stringContainingWc) {
        if(set.contains(stringContainingWc)) {
            return set.remove(stringContainingWc);
        } else {
            boolean modified = false;
            Set<IndexType> copy = new HashSet<IndexType>(set);
            
            for(IndexType it: copy) {
                if(WildcardMatcher.match(stringContainingWc.getCombinedString(), it.getCombinedString())) {
                    modified = set.remove(it) | modified;
                }
            }
            return modified;
        }  
    }
    
    private List<String> toString(List<AliasMetaData> aliases) {
        if(aliases == null || aliases.size() == 0) {
            return Collections.emptyList();
        }
        
        final List<String> ret = new ArrayList<String>(aliases.size());
        
        for(final AliasMetaData amd: aliases) {
            if(amd != null) {
                ret.add(amd.alias());
            }
        }
        
        return Collections.unmodifiableList(ret);
    }
    
    public boolean multitenancyEnabled() {
        return privilegesInterceptor.getClass() != PrivilegesInterceptor.class 
                && getConfigSettings().getAsBoolean("searchguard.dynamic.kibana.multitenancy_enabled", true);
    }
    
    public boolean notFailOnForbiddenEnabled() {
        return privilegesInterceptor.getClass() != PrivilegesInterceptor.class
                && getConfigSettings().getAsBoolean("searchguard.dynamic.kibana.do_not_fail_on_forbidden", false);
    }
    
    public String kibanaIndex() {
        return getConfigSettings().get("searchguard.dynamic.kibana.index",".kibana");
    }
    
    public String kibanaServerUsername() {
        return getConfigSettings().get("searchguard.dynamic.kibana.server_username","kibanaserver");
    }
    
    public boolean kibanaIndexReadonly(final User user, final TransportAddress caller) {
        final Set<String> sgRoles = mapSgRoles(user, caller);
        
        final String kibanaIndex = kibanaIndex();
        
        for (final Iterator<String> iterator = sgRoles.iterator(); iterator.hasNext();) {
            final String sgRole = iterator.next();
            final Settings sgRoleSettings = getRolesSettings().getByPrefix(sgRole);
            
            if (sgRoleSettings.names().isEmpty()) {
                continue;
            }

            final Map<String, Settings> permittedAliasesIndices0 = sgRoleSettings.getGroups(".indices");
            final Map<String, Settings> permittedAliasesIndices = new HashMap<String, Settings>(permittedAliasesIndices0.size());

            for (String origKey : permittedAliasesIndices0.keySet()) {
                permittedAliasesIndices.put(origKey.replace("${user.name}", user.getName()).replace("${user_name}", user.getName()),
                        permittedAliasesIndices0.get(origKey));
            }
            
            for(String indexPattern: permittedAliasesIndices.keySet()) {                
                if(WildcardMatcher.match(indexPattern, kibanaIndex)) {
                    final Settings innerSettings = permittedAliasesIndices.get(indexPattern);
                    final String[] perms = innerSettings.getAsArray("*");
                    if(perms!= null && perms.length > 0) {
                        if(WildcardMatcher.matchAny(resolveActions(perms).toArray(new String[0]), "indices:data/write/update")) {
                            return false;
                        }
                    }
                }
            }
        }

        return true;
    }

    private class TenantHolder implements ConfigurationChangeListener {

        private SetMultimap<String, Tuple<String, Boolean>> tenantsMM = null;
        private volatile boolean initialized = false;

        public Map<String, Boolean> mapTenants(final User user, Set<String> roles) {

            if (user == null || tenantsMM == null) {
                return Collections.emptyMap();
            }

            final Map<String, Boolean> result = new HashMap<>(roles.size());
            result.put(user.getName(), true);

            tenantsMM.entries().stream().filter(e -> roles.contains(e.getKey())).filter(e -> !user.getName().equals(e.getValue().v1())).forEach(e -> {
                final String tenant = e.getValue().v1();
                final boolean rw = e.getValue().v2();

                if (rw || !result.containsKey(tenant)) { //RW outperforms RO
                    result.put(tenant, rw);
                }
            });
            return Collections.unmodifiableMap(result);
        }

        @Override
        public void onChange(Settings roles) {

            final Set<Future<Tuple<String, Set<Tuple<String, Boolean>>>>> futures = new HashSet<>(roles.size());

            final ExecutorService execs = Executors.newFixedThreadPool(10);

            for (String sgRole : roles.names()) {

                Future<Tuple<String, Set<Tuple<String, Boolean>>>> future = execs.submit(new Callable<Tuple<String, Set<Tuple<String, Boolean>>>>() {
                    @Override
                    public Tuple<String, Set<Tuple<String, Boolean>>> call() throws Exception {
                        final Set<Tuple<String, Boolean>> tuples = new HashSet<>();
                        final Settings tenants = getRolesSettings().getByPrefix(sgRole + ".tenants.");

                        if (tenants != null) {
                            for (String tenant : tenants.names()) {

                                if ("RW".equalsIgnoreCase(tenants.get(tenant, "RO"))) {
                                    //RW
                                    tuples.add(new Tuple<String, Boolean>(tenant, true));
                                } else {
                                    //RO
                                    //if(!tenantsMM.containsValue(value)) { //RW outperforms RO
                                    tuples.add(new Tuple<String, Boolean>(tenant, false));
                                    //}
                                }
                            }
                        }

                        return new Tuple<String, Set<Tuple<String, Boolean>>>(sgRole, tuples);
                    }
                });

                futures.add(future);

            }

            execs.shutdown();
            try {
                execs.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted (1) while loading roles");
                return;
            }

            try {
                final SetMultimap<String, Tuple<String, Boolean>> tenantsMM_ = SetMultimapBuilder.hashKeys(futures.size()).hashSetValues(16).build();

                for (Future<Tuple<String, Set<Tuple<String, Boolean>>>> future : futures) {
                    Tuple<String, Set<Tuple<String, Boolean>>> result = future.get();
                    tenantsMM_.putAll(result.v1(), result.v2());
                }

                tenantsMM = tenantsMM_;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted (2) while loading roles");
                return;
            } catch (ExecutionException e) {
                log.error("Error while updating roles: {}", e.getCause(), e.getCause());
                throw ExceptionsHelper.convertToElastic(e);
            }
            
            initialized = true;

        }
    }

    private class RoleMappingHolder {

        private ListMultimap<String, String> users;
        private ListMultimap<Set<String>, String> abars;
        private ListMultimap<String, String> bars;
        private ListMultimap<String, String> hosts;

        private RoleMappingHolder(Settings rolesMapping) {

            if (rolesMapping != null) {

                final ListMultimap<String, String> users_ = ArrayListMultimap.create();
                final ListMultimap<Set<String>, String> abars_ = ArrayListMultimap.create();
                final ListMultimap<String, String> bars_ = ArrayListMultimap.create();
                final ListMultimap<String, String> hosts_ = ArrayListMultimap.create();

                for (final String roleMap : rolesMapping.names()) {

                    final Settings roleMapSettings = rolesMapping.getByPrefix(roleMap);

                    for (String u : roleMapSettings.getAsArray(".users", new String[0])) {
                        users_.put(u, roleMap);
                    }

                    final Set<String> abar = new HashSet<String>(Arrays.asList(roleMapSettings.getAsArray(".and_backendroles", new String[0])));

                    if (!abar.isEmpty()) {
                        abars_.put(abar, roleMap);
                    }

                    for (String bar : roleMapSettings.getAsArray(".backendroles", new String[0])) {
                        bars_.put(bar, roleMap);
                    }

                    for (String host : roleMapSettings.getAsArray(".hosts", new String[0])) {
                        hosts_.put(host, roleMap);
                    }
                }

                users = users_;
                abars = abars_;
                bars = bars_;
                hosts = hosts_;
            }
        }

        private Set<String> map(final User user, final TransportAddress caller) {

            if (user == null || users == null || abars == null || bars == null || hosts == null) {
                return Collections.emptySet();
            }

            final Set<String> sgRoles = new TreeSet<String>();


                for (String p : WildcardMatcher.getAllMatchingPatterns(users.keySet(), user.getName())) {
                    sgRoles.addAll(users.get(p));
                }

                for (String p : WildcardMatcher.getAllMatchingPatterns(bars.keySet(), user.getRoles())) {
                    sgRoles.addAll(bars.get(p));
                }

                for (Set<String> p : abars.keySet()) {
                    if (WildcardMatcher.allPatternsMatched(p, user.getRoles())) {
                        sgRoles.addAll(abars.get(p));
                    }
                }

                if (caller != null) {

                    for (String p : WildcardMatcher.getAllMatchingPatterns(hosts.keySet(), caller.getAddress())) {
                        sgRoles.addAll(hosts.get(p));
                    }

                    for (String p : WildcardMatcher.getAllMatchingPatterns(hosts.keySet(), caller.getHost())) {
                        sgRoles.addAll(hosts.get(p));
                    }

                    
                }
            

            return Collections.unmodifiableSet(sgRoles);

        }
    }
}
