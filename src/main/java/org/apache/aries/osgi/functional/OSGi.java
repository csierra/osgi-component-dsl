/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.aries.osgi.functional;

import org.apache.aries.osgi.functional.OSGiOperation.OSGiResult;
import org.apache.aries.osgi.functional.OSGiOperation.Tuple;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.Filter;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceObjects;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.cm.ManagedServiceFactory;
import org.osgi.util.tracker.BundleTracker;
import org.osgi.util.tracker.BundleTrackerCustomizer;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Carlos Sierra Andrés
 */
public class OSGi<T> {

	private static final Runnable NOOP = () -> {};

	protected OSGiOperation<T> _operation;

	public OSGi(OSGiOperation<T> operation) {
		_operation = operation;
	}

	public static void close(OSGiResult<?> osgiResult) {
		osgiResult.close.run();
	}

	public <S> OSGi<S> map(Function<T, S> function) {
		return new OSGi<>(((bundleContext) -> {
			OSGiResult<T> osgiResult = _operation.run(bundleContext);

			return new OSGiResult<>(
				osgiResult.added.map(t -> t.map(function)),
				osgiResult.removed.map(t -> t.map(function)),
				osgiResult.start, osgiResult.close);
		}));
	}

	public static <S> OSGi<S> just(S s) {
		return new OSGi<>(((bundleContext) -> {

			Pipe<Tuple<S>, Tuple<S>> added = Pipe.create();
			Consumer<Tuple<S>> source = added.getSource();

			return new OSGiResult<>(
				added, Pipe.create(),
				() -> source.accept(Tuple.create(s)), NOOP);
		}));
	}

	public static <S> OSGi<S> nothing() {
		return new OSGi<>(((bundleContext) -> new OSGiResult<>(
			Pipe.create(), Pipe.create(), NOOP, NOOP)));
	}

	public <S> OSGi<S> flatMap(Function<T, OSGi<S>> fun) {
		return new OSGi<>(
			((bundleContext) -> {

				Map<Object, OSGiResult<S>> identities = new IdentityHashMap<>();

				AtomicReference<Runnable> closeReference =
					new AtomicReference<>(NOOP);

				Pipe<Tuple<S>, Tuple<S>> added = Pipe.create();

				Consumer<Tuple<S>> addedSource = added.getSource();

				OSGiResult<S> osgiResult = new OSGiResult<>(
					added, Pipe.create(), null,
					() -> {
						synchronized (identities) {
							for (OSGiResult<S> result : identities.values()) {
								close(result);
							}
						}

						closeReference.get().run();
					});

				osgiResult.start = () -> {
					OSGiResult<T> or1 = _operation.run(bundleContext);

					closeReference.set(or1.close);

					or1.added.map(t -> {
						OSGi<S> program = fun.apply(t.t);

						OSGiResult<S> or2 = program._operation.run(
							bundleContext);

						identities.put(t.original, or2);

						or2.added.map(r -> {
							addedSource.accept(r);

							return null;
						});

						or2.start.run();

						return null;
					});

					or1.removed.map(t -> {
						synchronized (identities) {
							OSGiResult<S> osgiResult1 = identities.remove(
								t.original);

							if (osgiResult1 != null) {
								OSGi.close(osgiResult1);
							}
						}

						return null;
					});

					or1.start.run();
				};

				return osgiResult;
			}
		));
	}

	public <S> OSGi<S> then(OSGi<S> next) {
		return flatMap(ignored -> next);
	}

	public <S> OSGi<Void> foreach(Function<T, OSGi<S>> fun) {
		return this.flatMap(fun).map(x -> null);
	}

	public static OSGi<Dictionary<String, ?>> configurations(
		String factoryPid) {

		return new OSGi<>(bundleContext -> {
			Map<String, Tuple<Dictionary<String, ?>>> results =
				new ConcurrentHashMap<>();

			AtomicReference<ServiceRegistration<ManagedServiceFactory>>
				serviceRegistrationReference = new AtomicReference<>(null);

			Pipe<Tuple<Dictionary<String, ?>>, Tuple<Dictionary<String, ?>>>
				added = Pipe.create();

			Consumer<Tuple<Dictionary<String, ?>>> addedSource =
				added.getSource();

			Pipe<Tuple<Dictionary<String, ?>>, Tuple<Dictionary<String, ?>>>
				removed = Pipe.create();

			Consumer<Tuple<Dictionary<String, ?>>> removedSource =
				removed.getSource();

			Runnable start = () ->
				serviceRegistrationReference.set(bundleContext.registerService(
					ManagedServiceFactory.class, new ManagedServiceFactory() {
						@Override
						public String getName() {
							return "Functional OSGi Managed Service Factory";
						}

						@Override
						public void updated(
							String s, Dictionary<String, ?> dictionary)
							throws ConfigurationException {

							Tuple<Dictionary<String, ?>> tuple = Tuple.create(
								dictionary);

							Tuple<Dictionary<String, ?>> old = results.put(
								s, tuple);

							if (old != null) {
								removedSource.accept(old);
							}

							addedSource.accept(tuple);
						}

						@Override
						public void deleted(String s) {
							Tuple<Dictionary<String, ?>> tuple =
								results.remove(s);

							removedSource.accept(tuple);
						}
					},
					new Hashtable<String, Object>() {{
						put("service.pid", factoryPid);
					}}));


			return new OSGiResult<>(added, removed, start,
				() -> {
					serviceRegistrationReference.get().unregister();

					for (Tuple<Dictionary<String, ?>> tuple :
							results.values()) {

						removedSource.accept(tuple);
					}
				});
		});
	}

	public static OSGi<Dictionary<String, ?>> configuration(String pid) {
		return new OSGi<>(bundleContext -> {
			AtomicReference<Dictionary<String, ?>> atomicReference =
				new AtomicReference<>(null);

			AtomicReference<Tuple<Dictionary<String, ?>>> tupleAtomicReference =
				new AtomicReference<>(Tuple.create(null));

			AtomicReference<ServiceRegistration<ManagedService>>
				serviceRegistrationReferece = new AtomicReference<>(null);

			Pipe<Tuple<Dictionary<String, ?>>, Tuple<Dictionary<String, ?>>>
				added = Pipe.create();

			Consumer<Tuple<Dictionary<String, ?>>> addedSource =
				added.getSource();

			Pipe<Tuple<Dictionary<String, ?>>, Tuple<Dictionary<String, ?>>>
				removed = Pipe.create();

			Consumer<Tuple<Dictionary<String, ?>>> removedSource =
				removed.getSource();

			Runnable start = () ->
				serviceRegistrationReferece.set(
					bundleContext.registerService(
						ManagedService.class,
						properties -> {
							while (
								!atomicReference.compareAndSet(
									tupleAtomicReference.get().t,
									properties)
							){}

							Tuple<Dictionary<String, ?>> old =
								tupleAtomicReference.get();

							if (old != null) {
								removedSource.accept(old);
							}

							Tuple<Dictionary<String, ?>> tuple =
								Tuple.create(properties);

							if (old != null) {
								addedSource.accept(tuple);
							}

							tupleAtomicReference.set(tuple);
						},
						new Hashtable<String, Object>() {{
							put("service.pid", pid);
						}}));

			return new OSGiResult<>(
				added, removed, start,
				() -> serviceRegistrationReferece.get().unregister());
		});
	}

	public static OSGi<Void> onClose(Runnable action) {
		return
			new OSGi<>(bundleContext -> {
				Pipe<Tuple<Void>, Tuple<Void>> pipe = Pipe.create();

				return new OSGiResult<>(
					pipe, Pipe.create(),
					() -> pipe.getSource().accept(Tuple.create(null)),
					action::run);
			});
	}

	public static <T> OSGi<ServiceReference<T>> serviceReferences(
		Class<T> clazz, String filterString) {

		return new OSGi<>(bundleContext -> {
			Pipe<Tuple<ServiceReference<T>>, Tuple<ServiceReference<T>>> added =
				Pipe.create();

			Consumer<Tuple<ServiceReference<T>>> addedSource =
				added.getSource();

			Pipe<Tuple<ServiceReference<T>>, Tuple<ServiceReference<T>>>
				removed = Pipe.create();

			Consumer<Tuple<ServiceReference<T>>> removedSource =
				removed.getSource();

			ServiceTracker<T, Tuple<ServiceReference<T>>> serviceTracker =
				new ServiceTracker<T, Tuple<ServiceReference<T>>>(
					bundleContext,
					buildFilter(bundleContext, filterString, clazz), null) {

					@Override
					public Tuple<ServiceReference<T>> addingService(
						ServiceReference<T> reference) {

						Tuple<ServiceReference<T>> tuple = Tuple.create(
							reference);

						addedSource.accept(tuple);

						return tuple;
					}

					@Override
					public void removedService(
						ServiceReference<T> reference,
						Tuple<ServiceReference<T>> t) {

						super.removedService(reference, t);

						removedSource.accept(t);
					}
				};

			return new OSGiResult<>(
				added, removed, serviceTracker::open, serviceTracker::close);

		});
	}

	public static <T> OSGi<T> changeContext(
		BundleContext bundleContext, OSGi<T> program) {

		return new OSGi<>(b -> program._operation.run(bundleContext));
	}

	public static MOSGi<Bundle> bundles(int stateMask) {
		return new BundleMOSGi(stateMask);
	}


	private static Filter buildFilter(
		BundleContext bundleContext, String filterString, Class<?> clazz) {
		Filter filter;
		try {
			if (filterString == null) {
				filter = bundleContext.createFilter(
					"(objectClass=" + clazz.getName() + ")");
			}
			else {
				filter = bundleContext.createFilter(
					"(&(objectClass=" + clazz.getName() + ")" +
					filterString + ")");
			}
		}
		catch (InvalidSyntaxException e) {
			throw new RuntimeException(e);
		}
		return filter;
	}

	public static <T, S extends T> OSGi<ServiceRegistration<T>> register(
		Class<T> clazz, S service, Map<String, Object> properties) {

		return new OSGi<>(bundleContext -> {
			ServiceRegistration<T> serviceRegistration =
				bundleContext.registerService(
					clazz, service, new Hashtable<>(properties));

			Pipe<Tuple<ServiceRegistration<T>>, Tuple<ServiceRegistration<T>>>
				added = Pipe.create();

			Consumer<Tuple<ServiceRegistration<T>>> addedSource =
				added.getSource();

			Tuple<ServiceRegistration<T>> tuple = Tuple.create(
				serviceRegistration);

			return new OSGiResult<>(
				added, Pipe.create(),
				() -> addedSource.accept(tuple),
				() -> {
					try {
						serviceRegistration.unregister();
					}
					catch (Exception e) {
					}
				});
		});
	}

	public static <T> OSGiResult<T> runOsgi(
		BundleContext bundleContext, OSGi<T> program) {

		AtomicBoolean executed = new AtomicBoolean(false);

		OSGiResult<T> osgiResult = program._operation.run(bundleContext);

		Runnable close = () -> {
			boolean hasBeenExecuted = executed.getAndSet(true);

			if (!hasBeenExecuted) {
				osgiResult.close.run();
			}
		};

		osgiResult.start.run();

		return new OSGiResult<>(
			osgiResult.added, osgiResult.removed,
			osgiResult.start, close);
	}

	public static <T> MOSGi<T> services(Class<T> clazz) {
		return services(clazz, null);
	}

	public static <T> MOSGi<T> services(Class<T> clazz, String filterString) {
		return new ServicesMOSGi<>(clazz, filterString);
	}

	public static <T> MOSGi<ServiceObjects<T>> prototypes(Class<T> clazz) {
		return prototypes(clazz, null);
	}

	public static <T> MOSGi<ServiceObjects<T>> prototypes(
		Class<T> clazz, String filterString) {

		return new PrototypesMOSGi<>(clazz, filterString);
	}

	private static class BundleMOSGi extends MOSGi<Bundle> {

		private final int _stateMask;
		public BundleMOSGi(int stateMask) {
			super(bundleContext -> {
				Pipe<Tuple<Bundle>, Tuple<Bundle>> added = Pipe.create();

				Consumer<Tuple<Bundle>> addedSource = added.getSource();

				Pipe<Tuple<Bundle>, Tuple<Bundle>> removed = Pipe.create();

				Consumer<Tuple<Bundle>> removedSource = removed.getSource();

				BundleTracker<Tuple<Bundle>> bundleTracker =
					new BundleTracker<>(
						bundleContext, stateMask,
						new BundleTrackerCustomizer<Tuple<Bundle>>() {

							@Override
							public Tuple<Bundle> addingBundle(
								Bundle bundle, BundleEvent bundleEvent) {

								Tuple<Bundle> tuple = Tuple.create(bundle);

								addedSource.accept(tuple);

								return tuple;
							}

							@Override
							public void modifiedBundle(
								Bundle bundle, BundleEvent bundleEvent,
								Tuple<Bundle> tuple) {

								removedBundle(bundle, bundleEvent, tuple);

								addingBundle(bundle, bundleEvent);
							}

							@Override
							public void removedBundle(
								Bundle bundle, BundleEvent bundleEvent,
								Tuple<Bundle> tuple) {

								removedSource.accept(tuple);
							}
						});

				return new OSGiResult<>(
					added, removed, bundleTracker::open, bundleTracker::close);
			});
			_stateMask = stateMask;
		}

		@Override
		public <S> OSGi<S> flatMap(Function<Bundle, OSGi<S>> fun) {
			return new OSGi<>(bundleContext -> {
				Pipe<Tuple<S>, Tuple<S>> added = Pipe.create();

				Consumer<Tuple<S>> addedSource = added.getSource();

				Pipe<Tuple<S>, Tuple<S>> removed = Pipe.create();

				Consumer<Tuple<S>> removedSource = removed.getSource();

				BundleTracker<Tracked<Bundle, S>> bundleTracker =
					new BundleTracker<>(
						bundleContext, _stateMask,
						new BundleTrackerCustomizer<Tracked<Bundle, S>>() {

							@Override
							public Tracked<Bundle, S> addingBundle(
								Bundle bundle, BundleEvent bundleEvent) {

								OSGi<S> program = fun.apply(bundle);

								OSGiResult<S> result =
									program._operation.run(bundleContext);

								Tracked<Bundle, S> tracked = new Tracked<>();

								tracked.service = bundle;
								tracked.program = result;

								result.added.map(s -> {
									tracked.result = s;

									addedSource.accept(s);

									return s;
								});

								result.start.run();

								return tracked;
							}

							@Override
							public void modifiedBundle(
								Bundle bundle, BundleEvent bundleEvent,
								Tracked<Bundle, S> tracked) {

								removedBundle(bundle, bundleEvent, tracked);

								addingBundle(bundle, bundleEvent);
							}

							@Override
							public void removedBundle(
								Bundle bundle, BundleEvent bundleEvent,
								Tracked<Bundle, S> tracked) {

								close(tracked.program);

								if (tracked.result != null) {
									removedSource.accept(tracked.result);
								}
							}
						});

				return new OSGiResult<>(
					added, removed, bundleTracker::open, bundleTracker::close);

			});
		}

	}
	private static class PrototypesMOSGi<T> extends MOSGi<ServiceObjects<T>> {

		private final String _filterString;
		private final Class<T> _clazz;
		public PrototypesMOSGi(Class<T> clazz, String filterString) {
			super(bundleContext -> {
				Pipe<Tuple<ServiceObjects<T>>, Tuple<ServiceObjects<T>>> added =
					Pipe.create();

				Pipe<Tuple<ServiceObjects<T>>, Tuple<ServiceObjects<T>>>
					removed = Pipe.create();

				Consumer<Tuple<ServiceObjects<T>>> addedSource =
					added.getSource();

				Consumer<Tuple<ServiceObjects<T>>> removedSource =
					removed.getSource();

				ServiceTracker<T, Tuple<ServiceObjects<T>>> serviceTracker =
					new ServiceTracker<>(
						bundleContext,
						OSGi.buildFilter(bundleContext, filterString, clazz),
						new ServiceTrackerCustomizer<T, Tuple<ServiceObjects<T>>>() {
							@Override
							public Tuple<ServiceObjects<T>> addingService(
								ServiceReference<T> reference) {

								ServiceObjects<T> serviceObjects =
									bundleContext.getServiceObjects(reference);

								Tuple<ServiceObjects<T>> tuple =
									Tuple.create(serviceObjects);

								addedSource.accept(tuple);

								return tuple;
							}

							@Override
							public void modifiedService(
								ServiceReference<T> reference,
								Tuple<ServiceObjects<T>> service) {

								removedService(reference, service);

								addingService(reference);
							}

							@Override
							public void removedService(
								ServiceReference<T> reference,
								Tuple<ServiceObjects<T>> tuple) {

								removedSource.accept(tuple);
							}
						});

				return new OSGiResult<>(
					added, removed, serviceTracker::open,
					serviceTracker::close);
			});
			_filterString = filterString;
			_clazz = clazz;
		}

		@Override
		public <S> OSGi<S> flatMap(Function<ServiceObjects<T>, OSGi<S>> fun) {
			return new OSGi<>(bundleContext -> {
				Pipe<Tuple<S>, Tuple<S>> added = Pipe.create();

				Pipe<Tuple<S>, Tuple<S>> removed = Pipe.create();

				Consumer<Tuple<S>> addedSource = added.getSource();

				Consumer<Tuple<S>> removedSource = removed.getSource();

				ServiceTracker<T, Tracked<ServiceObjects<T>, S>>
					serviceTracker = new ServiceTracker<>(
						bundleContext,
						buildFilter(bundleContext, _filterString, _clazz),
						new ServiceTrackerCustomizer
							<T, Tracked<ServiceObjects<T>, S>>() {

							@Override
							public Tracked<ServiceObjects<T>, S> addingService(
								ServiceReference<T> reference) {

								ServiceObjects<T> serviceObjects =
									bundleContext.getServiceObjects(
										reference);

								OSGi<S> program = fun.apply(serviceObjects);

								OSGiResult<S> result =
									program._operation.run(bundleContext);

								Tracked<ServiceObjects<T>, S> tracked =
									new Tracked<>();

								tracked.service = serviceObjects;
								tracked.program = result;

								result.added.map(s -> {
									tracked.result = s;

									addedSource.accept(s);

									return s;
								});

								result.start.run();

								return tracked;
							}

							@Override
							public void modifiedService(
								ServiceReference<T> reference,
								Tracked<ServiceObjects<T>, S> tracked) {

								removedService(reference, tracked);

								addingService(reference);
							}

							@Override
							public void removedService(
								ServiceReference<T> reference,
								Tracked<ServiceObjects<T>, S> tracked) {

								close(tracked.program);

								if (tracked.result != null) {
									removedSource.accept(tracked.result);
								}
							}
						});

				return new OSGiResult<>(
					added, removed, serviceTracker::open,
					serviceTracker::close);
			});
		}

	}
	private static class ServicesMOSGi<T> extends MOSGi<T> {


		private final String _filterString;
		private final Class<T> _clazz;
		public ServicesMOSGi(Class<T> clazz, String filterString) {
			super(bundleContext -> {
				Pipe<Tuple<T>, Tuple<T>> added = Pipe.create();

				Pipe<Tuple<T>, Tuple<T>> removed = Pipe.create();

				Consumer<Tuple<T>> addedSource = added.getSource();

				Consumer<Tuple<T>> removedSource = removed.getSource();

				ServiceTracker<T, Tuple<T>> serviceTracker =
					new ServiceTracker<>(
						bundleContext,
						OSGi.buildFilter(bundleContext, filterString, clazz),
						new ServiceTrackerCustomizer<T, Tuple<T>>() {
							@Override
							public Tuple<T> addingService(
								ServiceReference<T> reference) {

								ServiceObjects<T> serviceObjects =
									bundleContext.getServiceObjects(reference);

								T service = serviceObjects.getService();

								Tuple<T> tuple = Tuple.create(service);

								addedSource.accept(tuple);

								return tuple;
							}

							@Override
							public void modifiedService(
								ServiceReference<T> reference,
								Tuple<T> service) {

								removedService(reference, service);

								addingService(reference);
							}

							@Override
							public void removedService(
								ServiceReference<T> reference, Tuple<T> tuple) {

								ServiceObjects<T> serviceObjects =
									bundleContext.getServiceObjects(reference);

								removedSource.accept(tuple);

								serviceObjects.ungetService(tuple.t);
							}
						});

				return new OSGiResult<>(
					added, removed, serviceTracker::open,
					serviceTracker::close);
			});
			_filterString = filterString;
			_clazz = clazz;
		}

		@Override
		public <S> OSGi<S> flatMap(Function<T, OSGi<S>> fun) {
			return new OSGi<>(bundleContext -> {
				Pipe<Tuple<S>, Tuple<S>> added = Pipe.create();

				Pipe<Tuple<S>, Tuple<S>> removed = Pipe.create();

				Consumer<Tuple<S>> addedSource = added.getSource();

				Consumer<Tuple<S>> removedSource = removed.getSource();

				ServiceTracker<T, Tracked<T, S>> serviceTracker =
					new ServiceTracker<>(
						bundleContext,
						buildFilter(bundleContext, _filterString, _clazz),
						new ServiceTrackerCustomizer<T, Tracked<T, S>>() {
							@Override
							public Tracked<T, S> addingService(
								ServiceReference<T> reference) {

								ServiceObjects<T> serviceObjects =
									bundleContext.getServiceObjects(
										reference);

								T service = serviceObjects.getService();

								OSGi<S> program = fun.apply(service);

								OSGiResult<S> result =
									program._operation.run(bundleContext);

								Tracked<T, S> tracked = new Tracked<>();

								tracked.service = service;
								tracked.program = result;

								result.added.map(s -> {
									tracked.result = s;

									addedSource.accept(s);

									return s;
								});

								result.start.run();

								return tracked;
							}

							@Override
							public void modifiedService(
								ServiceReference<T> reference,
								Tracked<T, S> tracked) {

								removedService(reference, tracked);

								addingService(reference);
							}

							@Override
							public void removedService(
								ServiceReference<T> reference,
								Tracked<T, S> tracked) {

								close(tracked.program);

								if (tracked.result != null) {
									removedSource.accept(tracked.result);
								}

								ServiceObjects<T> serviceObjects =
									bundleContext.getServiceObjects(
										reference);

								serviceObjects.ungetService(
									tracked.service);
							}
						});

				return new OSGiResult<>(
					added, removed, serviceTracker::open,
					serviceTracker::close);

			});
		}

	}

	public static class MOSGi<T> extends OSGi<T> {
		MOSGi(OSGiOperation<T> operation) {
			super(operation);
		}

		public OSGi<T> once() {
			AtomicReference<T> atomicReference = new AtomicReference<>(null);

			return flatMap(t -> {
				if (atomicReference.compareAndSet(null, t)) {
					return just(t);
				}
				else {
					return nothing();
				}
			});
		}
	}

	private static class Tracked<T, S> {
		T service = null;

		OSGiResult<S> program = null;

		Tuple<S> result = null;
	}

}


