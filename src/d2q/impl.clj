(ns d2q.impl
  "Algorithm with tabular, non-blocking resolvers. Allows for low-latency and avoiding blocking IO."
  (:require [manifold.deferred :as mfd]
            [d2q.impl.tabular-protocols :as tp]
            [d2q.impl.utils :as impl.utils])
  (:import (java.util List ArrayList)))

(defn q-clauses
  [query]
  query)

(defn qc-field-name [qc]
  (:d2q.fcall/field qc))

(defn qc-key [qc]
  (:d2q.fcall/key qc))

(defn qc-args [qc]
  (:d2q.fcall/args qc))

(defn qc-nested [qc]
  (:d2q.fcall/nested qc))

(defn table-resolver-for-field
  [field]
  (tp/field-table-resolver field)
  )

(defn resolve-table
  [tr qctx f+args o+is]
  (try
    (tp/resolve-table tr qctx f+args o+is)
    (catch Throwable err
      (mfd/error-deferred err))))

(defn field-by-name
  [engine field-name]
  (or
    (tp/field-by-name engine field-name)
    (throw (ex-info
             (str "No Field registered under name " (pr-str field-name))
             {:d2q.field/name field-name}))))

(defn scalar-typed?
  [field]
  (tp/field-scalar? field))

(defn ref-typed?
  [field]
  (not (scalar-typed? field)))

(defn many-typed?
  [field]
  (tp/field-many? field))

(defrecord EnrichedQClause
  [key field-name args nested field table-resolver src-qc])

(defn enrich-qc [engine qc]
  (let [field-n (qc-field-name qc)
        f (field-by-name engine field-n)
        tr (table-resolver-for-field f)]
    (->EnrichedQClause
      (qc-key qc)
      field-n
      (qc-args qc)
      (qc-nested qc)
      f
      tr
      qc)))

(defn merge-maps-arrs
  "Given a seq of l-sized arrays of maps, returns an array of maps merged together."
  #^"[Ljava.lang.Object;" [l map-arrays]
  (let [map-arrs (to-array map-arrays)
        ret-arr (object-array l)]
    (impl.utils/doarr-indexed! [[i _] ret-arr]
      (aset ret-arr i
        (persistent!
          (impl.utils/areduce-iv
            [[m _ ^objects map-arr] map-arrs]
            (transient {})
            (->> (aget map-arr i)
              (reduce-kv assoc! m)))
          )))
    ret-arr))

(comment
  (vec
    (merge-maps-arrs
      3
      (for [k [:a :b :c]]
        (object-array (for [i (range 4)] {k i})))))
  => [{:a 0, :b 0, :c 0} {:a 1, :b 1, :c 1} {:a 2, :b 2, :c 2}]
  )

(defn populate
  "`objs` is an array of DD objects, returns an array of corresponding DD results (maps or nil)."
  [engine, qctx, query, ^objects objs]
  (mfd/future
    (let [e-qs
          (mapv #(enrich-qc engine %) (q-clauses query))

          o+is
          (vec (impl.utils/amap-indexed [[i o] objs] [o i]))

          p_transformed-entities
          (-> (tp/transform-entities engine qctx query o+is)
            (mfd/chain vec)
            (mfd/catch
              (fn [^Throwable err]
                (throw (ex-info
                         (str "Error when transforming entities: " (pr-str (class err)) " : " (.getMessage err))
                         {:query query}
                         err)))))]
      (-> p_transformed-entities
        (mfd/chain
          (fn [o+is]
            (let [n-objs (alength objs)

                  by-tr (group-by :table-resolver e-qs)
                  p_table-batches
                  (->> by-tr
                    (map
                      (fn [[tr e-qs]]
                        (let [f+args (->> e-qs
                                       (map (fn [e-qc]
                                              [(:key e-qc) (:field-name e-qc) (:args e-qc)])))
                              p_results (-> (resolve-table tr qctx f+args o+is)
                                          (mfd/catch
                                            (fn [^Throwable err]
                                              (throw (ex-info
                                                       (str "Tabular Resolver " (pr-str (tp/tr-name tr))
                                                         " failed with " (pr-str (class err)) " : " (.getMessage err))
                                                       (merge
                                                         {:d2q.resolver/name (tp/tr-name tr)
                                                          :f+args f+args}
                                                         (ex-data err))
                                                       err)))))
                              {ref-e-qs true scalar-e-qs false} (group-by #(ref-typed? (:field %)) e-qs)]
                          (mfd/chain p_results
                            (fn [results]
                              (let [k->scalar
                                    (impl.utils/index-and-map-by
                                      :key (constantly true)
                                      scalar-e-qs)
                                    k->ref
                                    (impl.utils/index-and-map-by
                                      :key
                                      (fn [e-qc]
                                        (let [;; The list of all children via e-qc
                                              children-objs-list (ArrayList.)
                                              many? (-> e-qc :field many-typed?)
                                              ;; an array of arrays, in the case of a to-many relationship, holding the ordered result maps of the children.
                                              children-results-arrs (when many?
                                                                      (object-array n-objs))]
                                          [children-objs-list
                                           many?
                                           children-results-arrs
                                           e-qc]))
                                      ref-e-qs)

                                    arr-scalars
                                    (let [arr-scalars (object-array n-objs)]
                                      (doseq [[parent-i k v] results]
                                        (let [tm (or
                                                   (aget arr-scalars parent-i)
                                                   (transient {}))]
                                          (aset arr-scalars parent-i
                                            (if-let [_ (k->scalar k)]
                                              (assoc! tm k v)
                                              (if-let [[^List children-list, many?, ^objects children-results-arrs] (k->ref k)]
                                                (do
                                                  (if many?
                                                    (do
                                                      (.addAll children-list
                                                        (into []
                                                          (map-indexed
                                                            (fn [j child]
                                                              [child parent-i j]))
                                                          v))
                                                      (aset children-results-arrs parent-i (object-array (count v))))
                                                    (.add children-list [v parent-i]))
                                                  tm)
                                                (throw (ex-info
                                                         (str "Unidentified key " (pr-str k))
                                                         {:k k})))))))
                                      (impl.utils/doarr-indexed! [[i tm] arr-scalars]
                                        (when (some? tm)
                                          (aset arr-scalars i (persistent! tm))))
                                      arr-scalars)

                                    p_ref-arrs
                                    (->> k->ref
                                      (mapv (fn [[k [^List children-list, many?, ^objects children-results-arrs, e-qc]]]
                                              (let [children-objs (impl.utils/amap-indexed
                                                                    [[_ [o _]]
                                                                     (to-array children-list)]
                                                                    o)
                                                    ;; recursive call
                                                    p_populated (populate engine qctx (:nested e-qc) children-objs)]
                                                (mfd/chain p_populated
                                                  (fn [^objects i+rs]
                                                    (if many?
                                                      (do
                                                        (impl.utils/doarr-indexed! [[i r] i+rs]
                                                          (let [[_ parent-i j] (.get children-list (int i))]
                                                            (-> children-results-arrs
                                                              ^objects (aget parent-i)
                                                              (aset j r))))
                                                        (impl.utils/amap-indexed
                                                          [[_ a-children-maps]
                                                           children-results-arrs]
                                                          (when (some? a-children-maps)
                                                            {k (vec a-children-maps)})))
                                                      (let [arr (object-array n-objs)]
                                                        (impl.utils/doarr-indexed! [[i r] i+rs]
                                                          (let [[_ parent-i] (.get children-list (int i))]
                                                            (aset arr parent-i {k r})))
                                                        arr))))
                                                ))))]
                                (apply mfd/zip
                                  (doto (mfd/deferred) (deliver arr-scalars))
                                  p_ref-arrs)
                                )))
                          )))
                    (apply mfd/zip))]
              (mfd/chain p_table-batches
                (fn [arr-seqs]
                  (merge-maps-arrs n-objs (apply concat arr-seqs))))
              )))))))

(defn query
  [engine qctx normalized-query obj-root]
  (mfd/chain (populate engine qctx normalized-query (to-array [obj-root]))
    (fn [^objects i+rs]
      (-> i+rs (aget 0) (or {})))))

;; ------------------------------------------------------------------------------
;; Structs

(defrecord TabularResolver
  [name f]
  tp/ITabularResolver
  (tr-name [_] name)
  (resolve-table [this qctx f+args o+is]
    (f qctx f+args o+is)))

(defrecord Field
  [fieldName isScalar isMany tr acl]
  tp/IField
  (field-name [this] fieldName)
  (field-scalar? [this] isScalar)
  (field-many? [this] isMany)
  (field-table-resolver [this] tr)
  )

(defrecord Engine
  [fieldByNames transformEntitiesFn #_resolveAccessLevelsFn]
  tp/IEngine
  (transform-entities [this qctx query o+is]
    (transformEntitiesFn qctx query o+is))
  (field-by-name [this field-name]
    (get fieldByNames field-name)))

(defn engine
  [tabular-resolvers fields transform-entities-fn]
  {:pre [(fn? transform-entities-fn)]}
  (let [trs-by-name (->> tabular-resolvers
                      (map (fn [tr]
                             (->TabularResolver
                               (:d2q.resolver/name tr)
                               (:d2q.resolver/compute tr))))
                      (impl.utils/index-by :name))
        fields-by-name
        (->> fields
          (map (fn [f]
                 (->Field
                   (:d2q.field/name f)
                   (not (:d2q.field/ref? f))
                   (= :d2q.field.cardinality/many (:d2q.field/cardinality f))
                   (-> f :d2q.field/resolver
                     (or (throw (ex-info
                                  (str "Unregistered resolver " (pr-str (:d2q.field/resolver f))
                                    " referenced in field " (:d2q.field/name f))
                                  {:field f})))
                     trs-by-name)
                   (set (:bs.d2q.field/acl f))
                   )))
          (impl.utils/index-by :fieldName))]
    (->Engine fields-by-name transform-entities-fn)))


