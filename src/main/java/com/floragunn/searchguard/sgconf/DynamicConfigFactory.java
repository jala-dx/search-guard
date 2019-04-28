package com.floragunn.searchguard.sgconf;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.auth.internal.InternalAuthenticationBackend;
import com.floragunn.searchguard.configuration.ClusterInfoHolder;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.StaticResourceException;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v6.ActionGroupsV6;
import com.floragunn.searchguard.sgconf.impl.v6.ConfigV6;
import com.floragunn.searchguard.sgconf.impl.v6.InternalUserV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleMappingsV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleV6;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7;
import com.floragunn.searchguard.sgconf.impl.v7.InternalUserV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleMappingsV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;

public class DynamicConfigFactory implements Initializable, ConfigurationChangeListener {
    
    //the returned classes are stable and can be filled by different config versions
    //config versions can have or support different features which older versions do not support 
    //make sure  we have implementations which can deal with thousands of roles, rolesmappings, internal users and tenants 
    //all model are readonly/immutable
    //rest api operates directly on specific config version because it supports only the current one
    
    //sg_internal_users.yml may be empty
    //sg_tenants.yml may be empty
    //sg_roles_mapping.yml may be empty
    //sg_config.yml may be empty
    //sg_action_groups.yml may be empty
    //sg_roles.yml may be empty
    //can all be empty -> {}
    
    private static final SgDynamicConfiguration<RoleV7> staticRoles;
    private static final SgDynamicConfiguration<ActionGroupsV7> staticActionGroups;
    private static final SgDynamicConfiguration<TenantV7> staticTenants;
    
    static {
        try {
            JsonNode staticRolesJsonNode = DefaultObjectMapper.YAML_MAPPER.readTree(DynamicConfigFactory.class.getResourceAsStream("/static_config/static_roles.yml"));
            staticRoles = SgDynamicConfiguration.fromNode(staticRolesJsonNode, CType.ROLES, 2, 0, 0);
   
            JsonNode staticActionGroupsJsonNode = DefaultObjectMapper.YAML_MAPPER.readTree(DynamicConfigFactory.class.getResourceAsStream("/static_config/static_action_goups.yml"));
            staticActionGroups = SgDynamicConfiguration.fromNode(staticActionGroupsJsonNode, CType.ACTIONGROUPS, 2, 0, 0);
      
            JsonNode staticTenantsJsonNode = DefaultObjectMapper.YAML_MAPPER.readTree(DynamicConfigFactory.class.getResourceAsStream("/static_config/static_tenants.yml"));
            staticTenants = SgDynamicConfiguration.fromNode(staticTenantsJsonNode, CType.TENANTS, 2, 0, 0);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }
    
    public static final SgDynamicConfiguration<RoleV7> getStaticRoles() {
        return staticRoles.deepClone();
    }
    
    public static final SgDynamicConfiguration<ActionGroupsV7> getStaticActionGroups() {
        return staticActionGroups.deepClone();
    }
    
    public static final SgDynamicConfiguration<TenantV7> getStaticTenants() {
        return staticTenants.deepClone();
    }
    
    public static final SgDynamicConfiguration<?> addStatics(SgDynamicConfiguration<?> original) {
        if(original.getCType() == CType.ACTIONGROUPS) {
            original.add(getStaticActionGroups());
        }
        
        if(original.getCType() == CType.ROLES) {
            original.add(getStaticRoles());
        }
        
        if(original.getCType() == CType.TENANTS) {
            original.add(getStaticTenants());
        }
        
        return original;
    }
    
    protected final Logger log = LogManager.getLogger(this.getClass());
    private final ConfigurationRepository cr;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final List<DCFListener> listeners = new ArrayList<>();
    private final Settings esSettings;
    private final Path configPath;
    private final InternalAuthenticationBackend iab = new InternalAuthenticationBackend();

    SgDynamicConfiguration<?> config;
    
    public DynamicConfigFactory(ConfigurationRepository cr, final Settings esSettings, 
            final Path configPath, Client client, ThreadPool threadPool, ClusterInfoHolder cih) {
        super();
        this.cr = cr;
        this.esSettings = esSettings;
        this.configPath = configPath;
        registerDCFListener(this.iab);
        this.cr.subscribeOnChange(this);
    }
    
    @Override
    public void onChange(Map<CType, SgDynamicConfiguration<?>> typeToConfig) {

        SgDynamicConfiguration<?> actionGroups = cr.getConfiguration(CType.ACTIONGROUPS);
        config = cr.getConfiguration(CType.CONFIG);
        SgDynamicConfiguration<?> internalusers = cr.getConfiguration(CType.INTERNALUSERS);
        SgDynamicConfiguration<?> roles = cr.getConfiguration(CType.ROLES);
        SgDynamicConfiguration<?> rolesmapping = cr.getConfiguration(CType.ROLESMAPPING);
        SgDynamicConfiguration<?> tenants = cr.getConfiguration(CType.TENANTS);
        
        if(log.isDebugEnabled()) {
            String logmsg = "current config (because of "+typeToConfig.keySet()+")\n"+
            " actionGroups: "+actionGroups.getImplementingClass()+" with "+actionGroups.getCEntries().size()+" entries\n"+
            " config: "+config.getImplementingClass()+" with "+config.getCEntries().size()+" entries\n"+
            " internalusers: "+internalusers.getImplementingClass()+" with "+internalusers.getCEntries().size()+" entries\n"+
            " roles: "+roles.getImplementingClass()+" with "+roles.getCEntries().size()+" entries\n"+
            " rolesmapping: "+rolesmapping.getImplementingClass()+" with "+rolesmapping.getCEntries().size()+" entries\n"+
            " tenants: "+tenants.getImplementingClass()+" with "+tenants.getCEntries().size()+" entries";
            log.debug(logmsg);
            
        }
        
        
        
        if(config.getImplementingClass() == ConfigV7.class) {
                //statics
                
                if(roles.containsAny(staticRoles)) {
                    throw new StaticResourceException("Cannot override static roles");
                }
                if(!roles.add(staticRoles)) {
                    throw new StaticResourceException("Unable to load static roles");
                }

                log.debug("Static roles loaded ({})", staticRoles.getCEntries().size());

                if(actionGroups.containsAny(staticActionGroups)) {
                    throw new StaticResourceException("Cannot override static action groups");
                }
                if(!actionGroups.add(staticActionGroups)) {
                    throw new StaticResourceException("Unable to load static action groups");
                }
                

                log.debug("Static action groups loaded ({})", staticActionGroups.getCEntries().size());
                
                if(tenants.containsAny(staticTenants)) {
                    throw new StaticResourceException("Cannot override static tenants");
                }
                if(!tenants.add(staticTenants)) {
                    throw new StaticResourceException("Unable to load static tenants");
                }
                

                log.debug("Static tenants loaded ({})", staticTenants.getCEntries().size());

                log.debug("Static configuration loaded (total roles: {}/total action groups: {}/total tenants: {})", roles.getCEntries().size(), actionGroups.getCEntries().size(), tenants.getCEntries().size());

            

            //rebuild v7 Models
            DynamicConfigModel dcf = new DynamicConfigModelV7(getConfigV7(config), esSettings, configPath, iab);
            InternalUsersModel cfff = new InternalUsersModelV7((SgDynamicConfiguration<InternalUserV7>) internalusers);
            ConfigModel cf = new ConfigModelV7((SgDynamicConfiguration<RoleV7>) roles,(SgDynamicConfiguration<RoleMappingsV7>)rolesmapping, (SgDynamicConfiguration<ActionGroupsV7>)actionGroups, (SgDynamicConfiguration<TenantV7>) tenants,dcf, esSettings);
            
            
            
            
            
            //notify listeners
            
            for(DCFListener listener: listeners) {
                listener.onChanged(cf, dcf, cfff);
            }
        
        } else {
            
            
            /*if(SearchGuardPlugin.AUTO_MIGRATE_FROMV6) {
                log.warn("Will perform automatic in memory migration from v6 to v7 configs");
                
                try {
                    SgDynamicConfiguration<ActionGroupsV7> actionGroupsV7 = Migration.migrateActionGroups((SgDynamicConfiguration<ActionGroupsV6>) actionGroups);
                    SgDynamicConfiguration<ConfigV7> configV7 = Migration.migrateConfig((SgDynamicConfiguration<ConfigV6>) config);
                    SgDynamicConfiguration<InternalUserV7> internalUsersV7 = Migration.migrateInternalUsers((SgDynamicConfiguration<InternalUserV6>) internalusers);
                    SgDynamicConfiguration<RoleMappingsV7> rolesmappingV7= Migration.migrateRoleMappings((SgDynamicConfiguration<RoleMappingsV6>) rolesmapping);
                    Tuple<SgDynamicConfiguration<RoleV7>, SgDynamicConfiguration<TenantV7>> roleTenants7 = Migration.migrateRoles((SgDynamicConfiguration<RoleV6>) roles, (SgDynamicConfiguration<RoleMappingsV6>) rolesmapping);
                    
                    DynamicConfigModel dcf = new DynamicConfigModelV7(getConfigV7(configV7), esSettings, configPath, iab);
                    InternalUsersModel cfff = new InternalUsersModelV7((SgDynamicConfiguration<InternalUserV7>) internalUsersV7);
                    ConfigModel cf = new ConfigModelV7((SgDynamicConfiguration<RoleV7>) roleTenants7.v1(), rolesmappingV7, actionGroupsV7, roleTenants7.v2(),dcf, esSettings);
                    
                    //notify listeners
                    
                    for(DCFListener listener: listeners) {
                        listener.onChanged(cf, dcf, cfff);
                    }
                    
                    initialized.set(true);
                    return;
                } catch (MigrationException e) {
                    log.error("Unable to migrate because of {}",e.toString(),e);
                }
                
            }
            
            if(SearchGuardPlugin.DISALLOW_V6) {
                throw new RuntimeException("Not allowed to load v6 config");
            }*/
            
            
            //rebuild v6 Models
            DynamicConfigModel dcf = new DynamicConfigModelV6(getConfigV6(config), esSettings, configPath, iab);
            InternalUsersModel cfff = new InternalUsersModelV6((SgDynamicConfiguration<InternalUserV6>) internalusers);
            ConfigModel cf = new ConfigModelV6((SgDynamicConfiguration<RoleV6>) roles, (SgDynamicConfiguration<ActionGroupsV6>)actionGroups, (SgDynamicConfiguration<RoleMappingsV6>)rolesmapping, dcf, esSettings);
            
            //notify listeners
            
            for(DCFListener listener: listeners) {
                listener.onChanged(cf, dcf, cfff);
            }
            
            

        }

        initialized.set(true);
        
    }
    
    public String getLicenseString() {
        
        if(!isInitialized()) {
            throw new RuntimeException("Can not retrieve license because not initialized (yet)");
        }
        
        if(config.getImplementingClass() == ConfigV6.class) {
            SgDynamicConfiguration<ConfigV6> c = (SgDynamicConfiguration<ConfigV6>) config;
            return c.getCEntry("searchguard").dynamic.license;
        } else {
            SgDynamicConfiguration<ConfigV7> c = (SgDynamicConfiguration<ConfigV7>) config;
            return c.getCEntry("sg_config").dynamic.license;
        }
    }
    
    private static ConfigV6 getConfigV6(SgDynamicConfiguration<?> sdc) {
        @SuppressWarnings("unchecked")
        SgDynamicConfiguration<ConfigV6> c = (SgDynamicConfiguration<ConfigV6>) sdc;
        return c.getCEntry("searchguard");
    }
    
    private static ConfigV7 getConfigV7(SgDynamicConfiguration<?> sdc) {
        @SuppressWarnings("unchecked")
        SgDynamicConfiguration<ConfigV7> c = (SgDynamicConfiguration<ConfigV7>) sdc;
        return c.getCEntry("sg_config");
    }
    
    //how to determine current?
    
    
    //atomic updates over a complete configupdate call
    //atomic also in the sense of each listener
    
    @Override
    public final boolean isInitialized() {
        return initialized.get();
    }
    
    public void registerDCFListener(DCFListener listener) {
        listeners.add(listener);
    }
    
    public static interface DCFListener {
        void onChanged(ConfigModel cf, DynamicConfigModel dcf, InternalUsersModel cfff);
    }
    
    private static class InternalUsersModelV7 extends InternalUsersModel {
        
        SgDynamicConfiguration<InternalUserV7> configuration;
        
        public InternalUsersModelV7(SgDynamicConfiguration<InternalUserV7> configuration) {
            super();
            this.configuration = configuration;
        }

        @Override
        public boolean exists(String user) {
            return configuration.exists(user);
        }

        @Override
        public List<String> getBackenRoles(String user) {
            InternalUserV7 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getBackend_roles();
        }

        @Override
        public Map<String, String> getAttributes(String user) {
            InternalUserV7 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getAttributes();
        }

        @Override
        public String getDescription(String user) {
            InternalUserV7 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getDescription();
        }

        @Override
        public String getHash(String user) {
            InternalUserV7 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getHash();
        }
        
    }
    
    private static class InternalUsersModelV6 extends InternalUsersModel {
        
        SgDynamicConfiguration<InternalUserV6> configuration;
        
        

        public InternalUsersModelV6(SgDynamicConfiguration<InternalUserV6> configuration) {
            super();
            this.configuration = configuration;
        }

        @Override
        public boolean exists(String user) {
            return configuration.exists(user);
        }

        @Override
        public List<String> getBackenRoles(String user) {
            InternalUserV6 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getRoles();
        }

        @Override
        public Map<String, String> getAttributes(String user) {
            InternalUserV6 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getAttributes();
        }

        @Override
        public String getDescription(String user) {
            return null;
        }

        @Override
        public String getHash(String user) {
            InternalUserV6 tmp = configuration.getCEntry(user);
            return tmp==null?null:tmp.getHash();
        }
        
    }
   
}
