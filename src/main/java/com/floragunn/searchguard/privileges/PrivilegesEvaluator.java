/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.privileges;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider.AuthFromInternalAuthToken;
import com.floragunn.searchguard.resolver.IndexResolverReplacer;
import com.floragunn.searchguard.resolver.IndexResolverReplacer.Resolved;
import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory.DCFListener;
import com.floragunn.searchguard.sgconf.DynamicConfigModel;
import com.floragunn.searchguard.sgconf.InternalUsersModel;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.user.User;

public class PrivilegesEvaluator implements DCFListener {

    protected final Logger log = LogManager.getLogger(this.getClass());
    protected final Logger actionTrace = LogManager.getLogger("sg_action_trace");
    private final ClusterService clusterService;

    private final IndexNameExpressionResolver resolver;

    private final AuditLog auditLog;
    private ThreadContext threadContext;

    private PrivilegesInterceptor privilegesInterceptor;

    private final boolean checkSnapshotRestoreWritePrivileges;

    private final ClusterInfoHolder clusterInfoHolder;
    private ConfigModel configModel;
    private final IndexResolverReplacer irr;
    private final SnapshotRestoreEvaluator snapshotRestoreEvaluator;
    private final SearchGuardIndexAccessEvaluator sgIndexAccessEvaluator;
    private final TermsAggregationEvaluator termsAggregationEvaluator;
    private final DlsFlsEvaluator dlsFlsEvaluator;
    private final boolean enterpriseModulesEnabled;
    private DynamicConfigModel dcm;

    public PrivilegesEvaluator(final ClusterService clusterService, final ThreadPool threadPool,
            final ConfigurationRepository configurationRepository, final IndexNameExpressionResolver resolver, AuditLog auditLog,
            final Settings settings, final PrivilegesInterceptor privilegesInterceptor, final ClusterInfoHolder clusterInfoHolder,
            final IndexResolverReplacer irr, boolean enterpriseModulesEnabled) {

        super();
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.auditLog = auditLog;

        this.threadContext = threadPool.getThreadContext();
        this.privilegesInterceptor = privilegesInterceptor;

        this.checkSnapshotRestoreWritePrivileges = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES,
                ConfigConstants.SG_DEFAULT_CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES);

        this.clusterInfoHolder = clusterInfoHolder;
        this.irr = irr;
        snapshotRestoreEvaluator = new SnapshotRestoreEvaluator(settings, auditLog);
        sgIndexAccessEvaluator = new SearchGuardIndexAccessEvaluator(settings, auditLog, irr);
        dlsFlsEvaluator = new DlsFlsEvaluator(settings, threadPool);
        termsAggregationEvaluator = new TermsAggregationEvaluator();
        this.enterpriseModulesEnabled = enterpriseModulesEnabled;
    }

    @Override
    public void onChanged(ConfigModel cm, DynamicConfigModel dcm, InternalUsersModel ium) {
        this.dcm = dcm;
        this.configModel = cm;
    }

    private SgRoles getSgRoles(Set<String> roles) {
        return configModel.getSgRoles().filter(roles);
    }

    public boolean isInitialized() {
        return configModel != null && configModel.getSgRoles() != null && dcm != null;
    }

    public PrivilegesEvaluatorResponse evaluate(final User user, String action0, final ActionRequest request, Task task,
            AuthFromInternalAuthToken authFromInternalAuthToken) {

        if (!isInitialized()) {
            throw new ElasticsearchSecurityException("Search Guard is not initialized.");
        }

        if (action0.startsWith("internal:indices/admin/upgrade")) {
            action0 = "indices:admin/upgrade";
        }

        TransportAddress caller;
        Set<String> mappedRoles;
        SgRoles sgRoles;

        if (authFromInternalAuthToken == null) {
            caller = Objects.requireNonNull((TransportAddress) this.threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS));
            mappedRoles = mapSgRoles(user, caller);
            sgRoles = getSgRoles(mappedRoles);
        } else {
            caller = null;
            mappedRoles = authFromInternalAuthToken.getSgRoles().getRoleNames();
            sgRoles = authFromInternalAuthToken.getSgRoles();
        }

        final PrivilegesEvaluatorResponse presponse = new PrivilegesEvaluatorResponse();

        if (log.isDebugEnabled()) {
            log.debug("### evaluate permissions for {} on {}", user, clusterService.localNode().getName());
            log.debug("action: " + action0 + " (" + request.getClass().getSimpleName() + ")");
        }

        final Resolved requestedResolved = irr.resolveRequest(request);

        if (log.isDebugEnabled()) {
            log.debug("requestedResolved : {}", requestedResolved);
        }

        // check dlsfls 
        if (enterpriseModulesEnabled
                //&& (action0.startsWith("indices:data/read") || action0.equals(ClusterSearchShardsAction.NAME))
                && dlsFlsEvaluator.evaluate(request, clusterService, resolver, requestedResolved, user, sgRoles, presponse).isComplete()) {
            return presponse;
        }

        // check snapshot/restore requests 
        if (snapshotRestoreEvaluator.evaluate(request, task, action0, clusterInfoHolder, presponse).isComplete()) {
            return presponse;
        }

        // SG index access
        if (sgIndexAccessEvaluator.evaluate(request, task, action0, requestedResolved, presponse).isComplete()) {
            return presponse;
        }

        final boolean dnfofEnabled = dcm.isDnfofEnabled();

        if (log.isTraceEnabled()) {
            log.trace("dnfof enabled? {}", dnfofEnabled);
        }

        if (isClusterPerm(action0)) {
            if (!sgRoles.impliesClusterPermissionPermission(action0)) {
                presponse.missingPrivileges.add(action0);
                presponse.allowed = false;
                log.info("No {}-level perm match for {} {} [Action [{}]] [RolesChecked {}]", "cluster", user, requestedResolved, action0,
                        sgRoles.getRoleNames());
                log.info("No permissions for {}", presponse.missingPrivileges);
                return presponse;
            } else {

                if (request instanceof RestoreSnapshotRequest && checkSnapshotRestoreWritePrivileges) {
                    if (log.isDebugEnabled()) {
                        log.debug("Normally allowed but we need to apply some extra checks for a restore request.");
                    }
                } else {

                    if (privilegesInterceptor.getClass() != PrivilegesInterceptor.class) {

                        final Boolean replaceResult = privilegesInterceptor.replaceKibanaIndex(request, action0, user, dcm, requestedResolved,
                                mapTenants(user, mappedRoles));

                        if (log.isDebugEnabled()) {
                            log.debug("Result from privileges interceptor for cluster perm: {}", replaceResult);
                        }

                        if (replaceResult == Boolean.TRUE) {
                            auditLog.logMissingPrivileges(action0, request, task);
                            return presponse;
                        }

                        if (replaceResult == Boolean.FALSE) {
                            presponse.allowed = true;
                            return presponse;
                        }
                    }

                    if (dnfofEnabled && (action0.startsWith("indices:data/read/")) && !requestedResolved.getAllIndices().isEmpty()) {

                        if (requestedResolved.getAllIndices().isEmpty()) {
                            presponse.missingPrivileges.clear();
                            presponse.allowed = true;
                            return presponse;
                        }

                        Set<String> reduced = sgRoles.reduce(requestedResolved, user, new String[] { action0 }, resolver, clusterService);

                        if (reduced.isEmpty()) {
                            presponse.allowed = false;
                            return presponse;
                        }

                        if (irr.replace(request, true, reduced.toArray(new String[0]))) {
                            presponse.missingPrivileges.clear();
                            presponse.allowed = true;
                            return presponse;
                        }
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Allowed because we have cluster permissions for " + action0);
                    }
                    presponse.allowed = true;
                    return presponse;
                }

            }
        }

        if (isTenantPerm(action0)) {
            if (!hasTenantPermission(user, mappedRoles, action0)) {
                presponse.missingPrivileges.add(action0);
                presponse.allowed = false;
                log.info("No {}-level perm match for {} {} [Action [{}]] [RolesChecked {}]", "tenant", user, requestedResolved, action0, mappedRoles);
                log.info("No permissions for {}", presponse.missingPrivileges);
                return presponse;
            } else {
                presponse.allowed = true;
                return presponse;
            }
        }

        // term aggregations
        if (termsAggregationEvaluator.evaluate(requestedResolved, request, clusterService, user, sgRoles, resolver, presponse).isComplete()) {
            return presponse;
        }

        final Set<String> allIndexPermsRequired = evaluateAdditionalIndexPermissions(request, action0);
        final String[] allIndexPermsRequiredA = allIndexPermsRequired.toArray(new String[0]);

        if (log.isDebugEnabled()) {
            log.debug("requested {} from {}", allIndexPermsRequired, caller);
        }

        presponse.missingPrivileges.clear();
        presponse.missingPrivileges.addAll(allIndexPermsRequired);

        if (log.isDebugEnabled()) {
            log.debug("requested resolved indextypes: {}", requestedResolved);
        }

        if (log.isDebugEnabled()) {
            log.debug("sgr: {}", sgRoles.getRoleNames());
        }

        //TODO exclude sg index

        if (privilegesInterceptor.getClass() != PrivilegesInterceptor.class) {

            final Boolean replaceResult = privilegesInterceptor.replaceKibanaIndex(request, action0, user, dcm, requestedResolved,
                    mapTenants(user, mappedRoles));

            if (log.isDebugEnabled()) {
                log.debug("Result from privileges interceptor: {}", replaceResult);
            }

            if (replaceResult == Boolean.TRUE) {
                auditLog.logMissingPrivileges(action0, request, task);
                return presponse;
            }

            if (replaceResult == Boolean.FALSE) {
                presponse.allowed = true;
                return presponse;
            }
        }

        if (dnfofEnabled && (action0.startsWith("indices:data/read/") || action0.startsWith("indices:admin/mappings/fields/get")
                || action0.equals("indices:admin/shards/search_shards"))) {

            if (requestedResolved.getAllIndices().isEmpty()) {
                presponse.missingPrivileges.clear();
                presponse.allowed = true;
                return presponse;
            }

            Set<String> reduced = sgRoles.reduce(requestedResolved, user, allIndexPermsRequiredA, resolver, clusterService);

            if (reduced.isEmpty()) {

                if (dcm.isDnfofForEmptyResultsEnabled()) {
                    //ITT-1886
                    if (request instanceof SearchRequest) {
                        ((SearchRequest) request).indices(new String[0]);
                        ((SearchRequest) request).indicesOptions(IndicesOptions.fromOptions(true, true, false, false));
                        presponse.missingPrivileges.clear();
                        presponse.allowed = true;
                        return presponse;
                    }

                    if (request instanceof ClusterSearchShardsRequest) {
                        ((ClusterSearchShardsRequest) request).indices(new String[0]);
                        ((ClusterSearchShardsRequest) request).indicesOptions(IndicesOptions.fromOptions(true, true, false, false));
                        presponse.missingPrivileges.clear();
                        presponse.allowed = true;
                        return presponse;
                    }

                    if (request instanceof GetFieldMappingsRequest) {
                        ((GetFieldMappingsRequest) request).indices(new String[0]);
                        ((GetFieldMappingsRequest) request).indicesOptions(IndicesOptions.fromOptions(true, true, false, false));
                        presponse.missingPrivileges.clear();
                        presponse.allowed = true;
                        return presponse;
                    }
                }

                presponse.allowed = false;
                return presponse;
            }

            if (irr.replace(request, true, reduced.toArray(new String[0]))) {
                presponse.missingPrivileges.clear();
                presponse.allowed = true;
                return presponse;
            }
        }

        //not bulk, mget, etc request here
        boolean permGiven = false;

        if (log.isDebugEnabled()) {
            log.debug("sgr2: {}", sgRoles.getRoleNames());
        }

        if (dcm.isMultiRolespanEnabled()) {
            permGiven = sgRoles.impliesTypePermGlobal(requestedResolved, user, allIndexPermsRequiredA, resolver, clusterService);
        } else {
            permGiven = sgRoles.get(requestedResolved, user, allIndexPermsRequiredA, resolver, clusterService);

        }

        if (!permGiven) {
            log.info("No {}-level perm match for {} {} [Action [{}]] [RolesChecked {}]", "index", user, requestedResolved, action0,
                    sgRoles.getRoleNames());
            log.info("No permissions for {}", presponse.missingPrivileges);
        } else {

            if (checkFilteredAliases(requestedResolved.getAllIndices(), action0)) {
                presponse.allowed = false;
                return presponse;
            }

            if (log.isDebugEnabled()) {
                log.debug("Allowed because we have all indices permissions for " + action0);
            }
        }

        presponse.allowed = permGiven;
        return presponse;

    }

    public Set<String> mapSgRoles(final User user, final TransportAddress caller) {
        return this.configModel.mapSgRoles(user, caller);
    }

    public Map<String, Boolean> mapTenants(final User user, Set<String> roles) {
        return this.configModel.mapTenants(user, roles);
    }

    public Set<String> getAllConfiguredTenantNames() {
        return configModel.getAllConfiguredTenantNames();
    }

    public boolean multitenancyEnabled() {
        return privilegesInterceptor.getClass() != PrivilegesInterceptor.class && dcm.isKibanaMultitenancyEnabled();
    }

    public boolean notFailOnForbiddenEnabled() {
        return privilegesInterceptor.getClass() != PrivilegesInterceptor.class && dcm.isDnfofEnabled();
    }

    public String kibanaIndex() {
        return dcm.getKibanaIndexname();
    }

    public String kibanaServerUsername() {
        return dcm.getKibanaServerUsername();
    }

    private Set<String> evaluateAdditionalIndexPermissions(final ActionRequest request, final String originalAction) {
        //--- check inner bulk requests
        final Set<String> additionalPermissionsRequired = new HashSet<>();

        if (!isClusterPerm(originalAction)) {
            additionalPermissionsRequired.add(originalAction);
        }

        if (request instanceof ClusterSearchShardsRequest) {
            additionalPermissionsRequired.add(SearchAction.NAME);
        }

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

        if (request instanceof IndicesAliasesRequest) {
            IndicesAliasesRequest bsr = (IndicesAliasesRequest) request;
            for (AliasActions bir : bsr.getAliasActions()) {
                switch (bir.actionType()) {
                case REMOVE_INDEX:
                    additionalPermissionsRequired.add(DeleteIndexAction.NAME);
                    break;
                default:
                    break;
                }
            }
        }

        if (request instanceof CreateIndexRequest) {
            CreateIndexRequest cir = (CreateIndexRequest) request;
            if (cir.aliases() != null && !cir.aliases().isEmpty()) {
                additionalPermissionsRequired.add(IndicesAliasesAction.NAME);
            }
        }

        if (request instanceof RestoreSnapshotRequest && checkSnapshotRestoreWritePrivileges) {
            additionalPermissionsRequired.addAll(ConfigConstants.SG_SNAPSHOT_RESTORE_NEEDED_WRITE_PRIVILEGES);
        }

        if (actionTrace.isTraceEnabled() && additionalPermissionsRequired.size() > 1) {
            actionTrace.trace(("Additional permissions required: " + additionalPermissionsRequired));
        }

        if (log.isDebugEnabled() && additionalPermissionsRequired.size() > 1) {
            log.debug("Additional permissions required: " + additionalPermissionsRequired);
        }

        return Collections.unmodifiableSet(additionalPermissionsRequired);
    }

    private static boolean isClusterPerm(String action0) {
        // TODO tenant permissions

        return (action0.startsWith("searchguard:cluster:") || action0.startsWith("cluster:") || action0.startsWith("indices:admin/template/")

                || action0.startsWith(SearchScrollAction.NAME) || (action0.equals(BulkAction.NAME)) || (action0.equals(MultiGetAction.NAME))
                || (action0.equals(MultiSearchAction.NAME)) || (action0.equals(MultiTermVectorsAction.NAME)) || (action0.equals(ReindexAction.NAME))

        );
    }

    private static boolean isTenantPerm(String action0) {
        return action0.startsWith("searchguard:tenant:");
    }

    private boolean checkFilteredAliases(Set<String> requestedResolvedIndices, String action) {
        //check filtered aliases
        for (String requestAliasOrIndex : requestedResolvedIndices) {

            final List<AliasMetaData> filteredAliases = new ArrayList<AliasMetaData>();

            final IndexMetaData indexMetaData = clusterService.state().metaData().getIndices().get(requestAliasOrIndex);

            if (indexMetaData == null) {
                log.debug("{} does not exist in cluster metadata", requestAliasOrIndex);
                continue;
            }

            final ImmutableOpenMap<String, AliasMetaData> aliases = indexMetaData.getAliases();

            if (aliases != null && aliases.size() > 0) {

                if (log.isDebugEnabled()) {
                    log.debug("Aliases for {}: {}", requestAliasOrIndex, aliases);
                }

                final Iterator<String> it = aliases.keysIt();
                while (it.hasNext()) {
                    final String alias = it.next();
                    final AliasMetaData aliasMetaData = aliases.get(alias);

                    if (aliasMetaData != null && aliasMetaData.filteringRequired()) {
                        filteredAliases.add(aliasMetaData);
                        if (log.isDebugEnabled()) {
                            log.debug(alias + " is a filtered alias " + aliasMetaData.getFilter());
                        }
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug(alias + " is not an alias or does not have a filter");
                        }
                    }
                }
            }

            if (filteredAliases.size() > 1 && WildcardMatcher.match("indices:data/read/*search*", action)) {
                //TODO add queries as dls queries (works only if dls module is installed)
                final String faMode = dcm.getFilteredAliasMode();// getConfigSettings().dynamic.filtered_alias_mode;

                if (faMode.equals("warn")) {
                    log.warn("More than one ({}) filtered alias found for same index ({}). This is currently not recommended. Aliases: {}",
                            filteredAliases.size(), requestAliasOrIndex, toString(filteredAliases));
                } else if (faMode.equals("disallow")) {
                    log.error("More than one ({}) filtered alias found for same index ({}). This is currently not supported. Aliases: {}",
                            filteredAliases.size(), requestAliasOrIndex, toString(filteredAliases));
                    return true;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("More than one ({}) filtered alias found for same index ({}). Aliases: {}", filteredAliases.size(),
                                requestAliasOrIndex, toString(filteredAliases));
                    }
                }
            }
        } //end-for

        return false;
    }

    private List<String> toString(List<AliasMetaData> aliases) {
        if (aliases == null || aliases.size() == 0) {
            return Collections.emptyList();
        }

        final List<String> ret = new ArrayList<>(aliases.size());

        for (final AliasMetaData amd : aliases) {
            if (amd != null) {
                ret.add(amd.alias());
            }
        }

        return Collections.unmodifiableList(ret);
    }

    public Map<String, Boolean> evaluateKibanaApplicationPrivileges(User user, TransportAddress caller, Collection<String> privileges) {

        if (privileges == null || privileges.isEmpty() || user == null) {
            log.debug(() -> "Privileges or user empty");
            return Collections.emptyMap();
        }

        final Set<String> mappedRoles = mapSgRoles(user, caller);
        final Map<String, Set<String>> tenantsPerms = this.configModel.mapTenantPermissions(user, mappedRoles);

        final String requestedTenant = user.getRequestedTenant();

        if (requestedTenant != null && multitenancyEnabled()) {
            //non-global tenant
            log.debug(() -> "Non global tenant: " + requestedTenant);
            return evaluateTenantPrivileges(tenantsPerms.get(requestedTenant.equals(User.USER_TENANT) ? user.getName() : requestedTenant),
                    privileges);
        } else {
            //global tenant or no mt enabled
            log.debug(() -> "Global tenant");
            return evaluateTenantPrivileges(tenantsPerms.get("SGS_GLOBAL_TENANT"), privileges);
        }
    }

    private Map<String, Boolean> evaluateTenantPrivileges(Set<String> privilegesGranted, Collection<String> privilegesAskedFor) {
        log.debug(() -> "Check " + privilegesGranted + " against " + privilegesAskedFor);
        final Map<String, Boolean> result = new HashMap<>();
        for (String privilegeAskedFor : privilegesAskedFor) {

            if (privilegesGranted == null || privilegesGranted.isEmpty()) {
                result.put(privilegeAskedFor, false);
            } else {
                result.put(privilegeAskedFor, WildcardMatcher.matchAny(privilegesGranted, privilegeAskedFor));
            }
        }
        return Collections.unmodifiableMap(result);
    }

    public boolean hasTenantPermission(User user, Set<String> mappedRoles, String action) {
        Map<String, Set<String>> tenantsPerms = this.configModel.mapTenantPermissions(user, mappedRoles);

        String requestedTenant = "SGS_GLOBAL_TENANT";

        if (multitenancyEnabled() && Strings.isNotEmpty(user.getRequestedTenant())) {
            requestedTenant = user.getRequestedTenant();
        }

        Set<String> permissions = tenantsPerms.get(requestedTenant);

        if (permissions == null || permissions.isEmpty()) {
            return false;
        }

        return WildcardMatcher.matchAny(permissions, action);
    }

    public boolean isKibanaRbacEnabled() {
        return dcm.isKibanaRbacEnabled();
    }

}
