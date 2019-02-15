/*
 * Copyright 2011-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.sequoiadb.core.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.AbstractMap;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

/**
 * Default implementation of a {@link MappingContext} for SequoiaDB using {@link BasicSequoiadbPersistentEntity} and
 * {@link BasicSequoiadbPersistentProperty} as primary abstractions.
 *
 */
public class SequoiadbMappingContext extends AbstractMappingContext<BasicSequoiadbPersistentEntity<?>, SequoiadbPersistentProperty>
		implements ApplicationContextAware {

	private static final FieldNamingStrategy DEFAULT_NAMING_STRATEGY = PropertyNameFieldNamingStrategy.INSTANCE;

	private FieldNamingStrategy fieldNamingStrategy = DEFAULT_NAMING_STRATEGY;
	private ApplicationContext context;

	/**
	 * Creates a new {@link SequoiadbMappingContext}.
	 */
	public SequoiadbMappingContext() {
		setSimpleTypeHolder(SequoiadbSimpleTypes.HOLDER);
	}

	/**
	 * Configures the {@link FieldNamingStrategy} to be used to determine the field name if no manual mapping is applied.
	 * Defaults to a strategy using the plain property name.
	 * 
	 * @param fieldNamingStrategy the {@link FieldNamingStrategy} to be used to determine the field name if no manual
	 *          mapping is applied.
	 */
	public void setFieldNamingStrategy(FieldNamingStrategy fieldNamingStrategy) {
		this.fieldNamingStrategy = fieldNamingStrategy == null ? DEFAULT_NAMING_STRATEGY : fieldNamingStrategy;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#shouldCreatePersistentEntityFor(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected boolean shouldCreatePersistentEntityFor(TypeInformation<?> type) {
		return !SequoiadbSimpleTypes.HOLDER.isSimpleType(type.getType()) && !AbstractMap.class.isAssignableFrom(type.getType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.AbstractMappingContext#createPersistentProperty(java.lang.reflect.Field, java.beans.PropertyDescriptor, org.springframework.data.mapping.MutablePersistentEntity, org.springframework.data.mapping.SimpleTypeHolder)
	 */
	@Override
	public SequoiadbPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
																BasicSequoiadbPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		return new CachingSequoiadbPersistentProperty(field, descriptor, owner, simpleTypeHolder, fieldNamingStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.BasicMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation, org.springframework.data.mapping.model.MappingContext)
	 */
	@Override
	protected <T> BasicSequoiadbPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		BasicSequoiadbPersistentEntity<T> entity = new BasicSequoiadbPersistentEntity<T>(typeInformation);

		if (context != null) {
			entity.setApplicationContext(context);
		}

		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}
}
