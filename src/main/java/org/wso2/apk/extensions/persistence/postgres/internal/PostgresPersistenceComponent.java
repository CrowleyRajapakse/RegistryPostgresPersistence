/*
 *  Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.wso2.apk.extensions.persistence.postgres.internal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.wso2.apk.extensions.persistence.postgres.PostgresDBPersistentImpl;
import org.wso2.apk.extensions.persistence.postgres.utils.PostgresDBConnectionUtil;
import org.wso2.carbon.apimgt.api.APIManagerDatabaseException;
import org.wso2.carbon.apimgt.persistence.APIPersistence;

@Component(
        name = "org.wso2.apk.extensions.persistence.postgres",
        immediate = true)
public class PostgresPersistenceComponent {
    private ServiceRegistration serviceRegistration = null;
    private static final Log log = LogFactory.getLog(PostgresPersistenceComponent.class);

    @Activate
    protected void activate(ComponentContext context) {
        serviceRegistration = context.getBundleContext().registerService(APIPersistence.class.getName(),
                new PostgresDBPersistentImpl(), null);
        log.info("Postgres DB persistence initialized as the ServiceRegistration");
//        try {
//            PostgresDBConnectionUtil.initialize();
//        } catch (APIManagerDatabaseException e) {
//            throw new RuntimeException(e);
//        }
    }

    @Deactivate
    protected void deactivate(ComponentContext context) {
        if (serviceRegistration != null) {
            serviceRegistration.unregister();
        }
        if (log.isDebugEnabled()) {
            log.info("Postgres DB persistence service deactivated.");
        }
    }
}
