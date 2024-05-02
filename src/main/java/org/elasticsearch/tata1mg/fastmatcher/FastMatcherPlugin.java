/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.elasticsearch.tata1mg.fastmatcher;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.*;
import org.elasticsearch.script.FilterScript.LeafFactory;
import org.elasticsearch.search.lookup.SearchLookup;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * RoaringBitmap plugin that allows filtering documents
 * using a base64 encoded roaringbitmap list of integers.
 */
public class FastMatcherPlugin extends Plugin implements ScriptPlugin {

	@Override
	public ScriptEngine getScriptEngine(
			Settings settings,
			Collection<ScriptContext<?>> contexts
			) {
		return new MyFastMatcherEngine();
	}

	// tag::fast_matcher
	private static class MyFastMatcherEngine implements ScriptEngine {
		@Override
		public String getType() {
			return "fast_matcher";
		}

		@Override
		public <T> T compile(
				String scriptName,
				String scriptSource,
				ScriptContext<T> context,
				Map<String, String> params
				) {
			if (!context.equals(FilterScript.CONTEXT)) {
				throw new IllegalArgumentException(getType()
						+ " scripts cannot be used for context ["
						+ context.name + "]");
			}
			// in this case, we use the name fast_matcher
			if ("fast_matcher".equals(scriptSource)) {
				FilterScript.Factory factory = new FastMatcherFactory();
				return context.factoryClazz.cast(factory);
			}
			throw new IllegalArgumentException("Unknown script name "
					+ scriptSource);
		}

		@Override
		public void close() {
			// optionally close resources
		}

		@Override
		public Set<ScriptContext<?>> getSupportedContexts() {
			return Set.of(ScoreScript.CONTEXT);
		}

		private static class FastMatcherFactory implements FilterScript.Factory,
		ScriptFactory {
			@Override
			public boolean isResultDeterministic() {
				// FastMatcherLeafFactory only uses deterministic APIs, this
				// implies the results are cacheable.
				return true;
			}

			@Override
			public LeafFactory newFactory(
					Map<String, Object> params,
					SearchLookup lookup
					) {
				final byte[] decodedTerms = Base64.getDecoder().decode(params.get("terms").toString());
				final ByteBuffer buffer = ByteBuffer.wrap(decodedTerms);
				RoaringBitmap rBitmap = new RoaringBitmap();
				try {
					rBitmap.deserialize(buffer);
				}
				catch (IOException e) {
					// Do something here
				}
				return new FastMatcherLeafFactory(params, lookup, rBitmap);
			}
		}

		private static class FastMatcherLeafFactory implements LeafFactory {
			private final Map<String, Object> params;
			private final SearchLookup lookup;
			private final String fieldName;
			private final String opType;
			private final RoaringBitmap rBitmap;
			private final boolean include;
			private final boolean exclude;

			private FastMatcherLeafFactory(Map<String, Object> params, SearchLookup lookup, RoaringBitmap rBitmap) {
				if (!params.containsKey("field")) {
					throw new IllegalArgumentException(
							"Missing parameter [field]");
				}
				if (!params.containsKey("terms")) {
					throw new IllegalArgumentException(
							"Missing parameter [terms]");
				}
				this.params = params;
				this.lookup = lookup;
				this.rBitmap = rBitmap;
				opType = params.get("operation").toString();
				fieldName = params.get("field").toString();
				include = opType.equals("include");
				exclude = !include;
			}


			@Override
			public FilterScript newInstance(DocReader docReader)
					throws IOException {
				DocValuesDocReader dvReader = ((DocValuesDocReader) docReader);
				BinaryDocValues binaryDocValues = dvReader.getLeafReaderContext().reader().getBinaryDocValues(fieldName);

				if (binaryDocValues == null) {
					/*
					 * the field and/or docValues doesn't exist in this segment
					 */
					return new FilterScript(params, lookup, docReader) {
						@Override
						public boolean execute() {
							// return true when used as exclude filter
							return exclude;
						}
					};
				}

				return new FilterScript(params, lookup, docReader) {
				    int currentDocid = -1;
					@Override
					public void setDocument(int docid) {
						/*
						 * advance has undefined behavior calling with
						 * a docid <= its current docid
						 */
						if (binaryDocValues.docID() < docid) {
						try {
							binaryDocValues.advance(docid);
						} catch (IOException e) {
							throw ExceptionsHelper.convertToElastic(e);
						}
						}
						currentDocid = docid;
					}

                                        public int getByteSize(long x) {
                                            if (x < 0) throw new IllegalArgumentException();
                                            int s = 1;
                                            while (s < 8 && x >= (1L << (s * 8))) s++;
                                            return s;
                                        }

					@Override
					public boolean execute() {
						final BytesRef docVal;
						try {
							docVal = binaryDocValues.binaryValue();
						} catch (IOException e) {
							throw ExceptionsHelper.convertToElastic(e);
						}
						if (docVal == null) {
							return !include;
						}
                                        int bytesNeededForLength = getByteSize(docVal.length);
                                        int extraPrefixBytes = bytesNeededForLength + 1;
        				final ByteBuffer buffer = ByteBuffer.wrap(docVal.bytes, docVal.offset + extraPrefixBytes, docVal.length - extraPrefixBytes);
				        RoaringBitmap rDocValBitmap = new RoaringBitmap();
                        try {
                            rDocValBitmap.deserialize(buffer);
                        }
                        catch (IOException e) {
                            throw ExceptionsHelper.convertToElastic(e);
                        }

						if (include) {
							return RoaringBitmap.intersects(rDocValBitmap, rBitmap);
						}
						else {
							return !RoaringBitmap.intersects(rDocValBitmap, rBitmap);
						}
					}
				};
			}
		}
	}
	// end::fast_matcher
}
