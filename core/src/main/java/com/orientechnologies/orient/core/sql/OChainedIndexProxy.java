/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerStub;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 * There are some cases when we need to create index for some class by traversed property. Unfortunately, such functionality is not
 * supported yet. But we can do that by creating index for each element of {@link OSQLFilterItemField.FieldChain} (which define
 * "way" to our property), and then process operations consequently using previously created indexes.
 * </p>
 * <p>
 * This class provides possibility to find optimal chain of indexes and then use it just like it was index for traversed property.
 * </p>
 * <p>
 * IMPORTANT: this class is only for internal usage!
 * </p>
 *
 * @author Artem Orobets
 */

@SuppressWarnings({ "unchecked", "rawtypes" })
public class OChainedIndexProxy<T> implements OIndex<T> {
  private final OIndex<T> firstIndex;

  private final List<OIndex<?>> indexChain;
  private final OIndex<?>       lastIndex;

  private OChainedIndexProxy(List<OIndex<?>> indexChain) {
    this.firstIndex = (OIndex<T>) indexChain.get(0);
    this.indexChain = Collections.unmodifiableList(indexChain);
    lastIndex = indexChain.get(indexChain.size() - 1);
  }

  /**
   * Create proxies that support maximum number of different operations. In case when several different indexes which support
   * different operations (e.g. indexes of {@code UNIQUE} and {@code FULLTEXT} types) are possible, the creates the only one index
   * of each type.
   *
   * @param longChain - property chain from the query, which should be evaluated
   *
   * @return proxies needed to process query.
   */
  public static <T> Collection<OChainedIndexProxy<T>> createProxies(OClass iSchemaClass, OSQLFilterItemField.FieldChain longChain) {
    List<OChainedIndexProxy<T>> proxies = new ArrayList<>();

    for (List<OIndex<?>> indexChain : getIndexesForChain(iSchemaClass, longChain)) {
      //noinspection ObjectAllocationInLoop
      proxies.add(new OChainedIndexProxy<>(indexChain));
    }

    return proxies;
  }

  private static boolean isComposite(OIndex<?> currentIndex) {
    return currentIndex.getDefinition().getParamCount() > 1;
  }

  private static Iterable<List<OIndex<?>>> getIndexesForChain(OClass iSchemaClass, OSQLFilterItemField.FieldChain fieldChain) {
    List<OIndex<?>> baseIndexes = prepareBaseIndexes(iSchemaClass, fieldChain);

    if (baseIndexes == null)
      return Collections.emptyList();

    Collection<OIndex<?>> lastIndexes = prepareLastIndexVariants(iSchemaClass, fieldChain);

    Collection<List<OIndex<?>>> result = new ArrayList<>();
    for (OIndex<?> lastIndex : lastIndexes) {
      @SuppressWarnings("ObjectAllocationInLoop")
      final List<OIndex<?>> indexes = new ArrayList<>(fieldChain.getItemCount());
      indexes.addAll(baseIndexes);
      indexes.add(lastIndex);

      result.add(indexes);
    }

    return result;
  }

  private static Collection<OIndex<?>> prepareLastIndexVariants(OClass iSchemaClass, OSQLFilterItemField.FieldChain fieldChain) {
    OClass oClass = iSchemaClass;
    final Collection<OIndex<?>> result = new ArrayList<>();

    for (int i = 0; i < fieldChain.getItemCount() - 1; i++) {
      oClass = oClass.getProperty(fieldChain.getItemName(i)).getLinkedClass();
      if (oClass == null) {
        return result;
      }
    }

    final Set<OIndex<?>> involvedIndexes = new TreeSet<>(Comparator.comparingInt(o -> o.getDefinition().getParamCount()));

    involvedIndexes.addAll(oClass.getInvolvedIndexes(fieldChain.getItemName(fieldChain.getItemCount() - 1)));
    final Collection<Class<? extends OIndex>> indexTypes = new HashSet<>(3);

    for (OIndex<?> involvedIndex : involvedIndexes) {
      if (!indexTypes.contains(involvedIndex.getInternal().getClass())) {
        result.add(involvedIndex);
        indexTypes.add(involvedIndex.getInternal().getClass());
      }
    }

    return result;
  }

  private static List<OIndex<?>> prepareBaseIndexes(OClass iSchemaClass, OSQLFilterItemField.FieldChain fieldChain) {
    List<OIndex<?>> result = new ArrayList<>(fieldChain.getItemCount() - 1);

    OClass oClass = iSchemaClass;
    for (int i = 0; i < fieldChain.getItemCount() - 1; i++) {
      final Set<OIndex<?>> involvedIndexes = oClass.getInvolvedIndexes(fieldChain.getItemName(i));
      final OIndex<?> bestIndex = findBestIndex(involvedIndexes);

      if (bestIndex == null)
        return null;

      result.add(bestIndex);
      oClass = oClass.getProperty(fieldChain.getItemName(i)).getLinkedClass();
    }
    return result;
  }

  /**
   * Finds the index that fits better as a base index in chain. Requirements to the base index:
   * <ul>
   * <li>Should be unique or not unique. Other types cannot be used to get all documents with required links.</li>
   * <li>Should not be composite hash index. As soon as hash index does not support partial match search.</li>
   * <li>Composite index that ignores null values should not be used.</li>
   * <li>Hash index is better than tree based indexes.</li>
   * <li>Non composite indexes is better that composite.</li>
   * </ul>
   *
   * @param indexes where search
   *
   * @return the index that fits better as a base index in chain
   */
  protected static OIndex<?> findBestIndex(Iterable<OIndex<?>> indexes) {
    OIndex<?> bestIndex = null;
    for (OIndex<?> index : indexes) {
      if (priorityOfUsage(index) > priorityOfUsage(bestIndex))
        bestIndex = index;
    }
    return bestIndex;
  }

  private static int priorityOfUsage(OIndex<?> index) {
    if (index == null)
      return -1;

    final OClass.INDEX_TYPE indexType = OClass.INDEX_TYPE.valueOf(index.getType());
    final boolean isComposite = isComposite(index);
    final boolean supportNullValues = supportNullValues(index);

    int priority = 1;

    if (isComposite) {
      if (!supportNullValues)
        return -1;
    } else {
      priority += 10;
    }

    switch (indexType) {
    case UNIQUE_HASH_INDEX:
    case NOTUNIQUE_HASH_INDEX:
      if (isComposite)
        return -1;
      else
        priority += 10;
      break;
    case UNIQUE:
    case NOTUNIQUE:
      priority += 5;
      break;
    case PROXY:
    case FULLTEXT:
      //noinspection deprecation
    case DICTIONARY:
    case FULLTEXT_HASH_INDEX:
    case DICTIONARY_HASH_INDEX:
    case SPATIAL:
      return -1;
    }

    return priority;
  }

  /**
   * Checks if index can be used as base index. Requirements to the base index:
   * <ul>
   * <li>Should be unique or not unique. Other types cannot be used to get all documents with required links.</li>
   * <li>Should not be composite hash index. As soon as hash index does not support partial match search.</li>
   * <li>Composite index that ignores null values should not be used.</li>
   * </ul>
   *
   * @param index to check
   *
   * @return true if index usage is allowed as base index.
   */
  public static boolean isAppropriateAsBase(OIndex<?> index) {
    return priorityOfUsage(index) > 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getRebuildVersion() {
    long rebuildVersion = 0;

    for (OIndex<?> index : indexChain) {
      rebuildVersion += index.getRebuildVersion();
    }

    return rebuildVersion;
  }

  private static boolean supportNullValues(OIndex<?> index) {
    final ODocument metadata = index.getMetadata();
    if (metadata == null)
      return false;

    final Boolean ignoreNullValues = metadata.field("ignoreNullValues");
    return Boolean.FALSE.equals(ignoreNullValues);
  }

  public String getDatabaseName() {
    return firstIndex.getDatabaseName();
  }

  public List<String> getIndexNames() {
    final ArrayList<String> names = new ArrayList<>(indexChain.size());
    for (OIndex<?> oIndex : indexChain) {
      names.add(oIndex.getName());
    }

    return names;
  }

  @Override
  public String getName() {
    final StringBuilder res = new StringBuilder("IndexChain{");
    final List<String> indexNames = getIndexNames();

    for (int i = 0; i < indexNames.size(); i++) {
      String indexName = indexNames.get(i);
      if (i > 0)
        res.append(", ");
      res.append(indexName);
    }

    res.append("}");

    return res.toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T get(Object iKey) {
    final Object lastIndexResult = lastIndex.get(iKey);

    final Set<OIdentifiable> result = new HashSet<>();

    if (lastIndexResult != null)
      result.addAll(applyTailIndexes(lastIndexResult));

    return (T) result;
  }

  /**
   * Returns internal index of last chain index, because proxy applicable to all operations that last index applicable.
   */
  public OIndexInternal<T> getInternal() {
    return (OIndexInternal<T>) lastIndex.getInternal();
  }

  /**
   * {@inheritDoc}
   */
  public OIndexDefinition getDefinition() {
    return lastIndex.getDefinition();
  }

  private List<ORID> applyTailIndexes(final Object lastIndexResult) {
    final OIndex<?> beforeTheLastIndex = indexChain.get(indexChain.size() - 2);
    Set<Comparable> currentKeys = prepareKeys(beforeTheLastIndex, lastIndexResult);

    for (int j = indexChain.size() - 2; j > 0; j--) {
      final OIndex<?> currentIndex = indexChain.get(j);
      final OIndex<?> nextIndex = indexChain.get(j - 1);

      final Set<Comparable> newKeys;
      if (isComposite(currentIndex)) {
        //noinspection ObjectAllocationInLoop
        newKeys = new TreeSet<>();
        for (Comparable currentKey : currentKeys) {
          final List<ORID> currentResult = getFromCompositeIndex(currentKey, currentIndex);
          newKeys.addAll(prepareKeys(nextIndex, currentResult));
        }
      } else {
        @SuppressWarnings("ObjectAllocationInLoop")
        final List<OIdentifiable> keys = currentIndex.iterateEntries(currentKeys, true).map((pair) -> pair.second)
            .collect(Collectors.toList());
        newKeys = prepareKeys(nextIndex, keys);
      }

      updateStatistic(currentIndex);

      currentKeys = newKeys;
    }

    return applyFirstIndex(currentKeys);
  }

  private List<ORID> applyFirstIndex(Collection<Comparable> currentKeys) {
    final List<ORID> result;
    if (isComposite(firstIndex)) {
      result = new ArrayList<>();
      for (Comparable key : currentKeys) {
        result.addAll(getFromCompositeIndex(key, firstIndex));
      }
    } else {
      result = firstIndex.iterateEntries(currentKeys, true).map((pair) -> pair.second).collect(Collectors.toList());
    }

    updateStatistic(firstIndex);

    return result;
  }

  private static List<ORID> getFromCompositeIndex(Comparable currentKey, OIndex<?> currentIndex) {
    return currentIndex.iterateEntriesBetween(currentKey, true, currentKey, true, true).map((pair) -> pair.second)
        .collect(Collectors.toList());
  }

  /**
   * Make type conversion of keys for specific index.
   *
   * @param index - index for which keys prepared for.
   * @param keys  - which should be prepared.
   *
   * @return keys converted to necessary type.
   */
  private static Set<Comparable> prepareKeys(OIndex<?> index, Object keys) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    if (keys instanceof Collection) {
      final Set<Comparable> newKeys = new TreeSet<>();
      for (Object o : ((Collection) keys)) {
        newKeys.add((Comparable) indexDefinition.createValue(o));
      }
      return newKeys;
    } else {
      return Collections.singleton((Comparable) indexDefinition.createValue(keys));
    }
  }

  /**
   * Register statistic information about usage of index in {@link OProfilerStub}.
   *
   * @param index which usage is registering.
   */
  private static void updateStatistic(OIndex<?> index) {

    final OProfiler profiler = Orient.instance().getProfiler();
    if (profiler.isRecording()) {
      Orient.instance().getProfiler()
          .updateCounter(profiler.getDatabaseMetric(index.getDatabaseName(), "query.indexUsed"), "Used index in query", +1);

      final int paramCount = index.getDefinition().getParamCount();
      if (paramCount > 1) {
        final String profiler_prefix = profiler.getDatabaseMetric(index.getDatabaseName(), "query.compositeIndexUsed");
        profiler.updateCounter(profiler_prefix, "Used composite index in query", +1);
        profiler
            .updateCounter(profiler_prefix + "." + paramCount, "Used composite index in query with " + paramCount + " params", +1);
      }
    }
  }

  //
  // Following methods are not allowed for proxy.
  //

  @Override
  public OIndex<T> create(String name, OIndexDefinition indexDefinition, String clusterIndexName, Set<String> clustersToIndex,
      boolean rebuild, OProgressListener progressListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean contains(Object iKey) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public OType[] getKeyTypes() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Iterator<Map.Entry<Object, T>> iterator() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public OIndex<T> put(Object iKey, OIdentifiable iValue) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean remove(Object key) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean remove(Object iKey, OIdentifiable iRID) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated Manual indexes are deprecated and will be removed
   */
  @Deprecated
  @Override
  public OIndex<T> clear() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public long getSize() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public long count(Object iKey) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public long getKeySize() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public OIndex<T> delete() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public String getType() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public String getAlgorithm() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean isAutomatic() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public long rebuild() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public long rebuild(OProgressListener iProgressListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public ODocument getConfiguration() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public ODocument getMetadata() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Set<String> getClusters() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Object getFirstKey() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Object getLastKey() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public int getIndexId() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public boolean isUnique() {
    return firstIndex.isUnique();
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descCursor() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Stream<Object> keyStream() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public int getVersion() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    return applyTailIndexes(lastIndex.iterateEntries(keys, ascSortOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    return applyTailIndexes(lastIndex.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return applyTailIndexes(lastIndex.iterateEntriesMajor(fromKey, fromInclusive, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    return applyTailIndexes(lastIndex.iterateEntriesMinor(toKey, toInclusive, ascOrder));
  }

  @Override
  public boolean isRebuilding() {
    return false;
  }

  private Stream<ORawPair<Object, ORID>> applyTailIndexes(Stream<ORawPair<Object, ORID>> indexStream) {
    return indexStream.flatMap((entry) -> applyTailIndexes(entry.second).stream().map((rid) -> new ORawPair<>(null, rid)));
  }

  @Override
  public int compareTo(OIndex<T> o) {
    throw new UnsupportedOperationException();
  }
}
