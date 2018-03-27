(ns d2q.test.api
  (:require [clojure.test :refer :all]
            [midje.sweet :refer :all]

            [d2q.api :as d2q :refer :all]))

(def db-example
  {:db/persons
   {"john-doe"
    {:person/id "john-doe"
     :person/email "john.doe@gmail.com"
     :person/age 18
     :person/address {:address/number "48"
                      :address/street "rue de Rome"
                      :address/city "Paris"}
     :person/notes ["blah" :blah 42]
     :animal/loves #{"alice-hacker" "minou"}}
    "alice-hacker"
    {:person/id "alice-hacker"
     :person/email "alice.hacker@gmail.com"
     :person/gender :person.gender/female
     :person/notes []
     :person/address nil}
    "bob-moran"
    {:person/id "bob-moran"
     :person/email "bob.moran@gmail.com"
     :person/age nil
     :person/gender :person.gender/male
     :person/address {:address/number "17"
                      :address/street "rue de Mars"
                      :address/city "Orléans"}}}
   :db/cats
   {"minou"
    {:cat/id "minou"
     :cat/name "Minou"
     :cat/owner "john-doe"
     :animal/loves #{"john-doe" "alice-hacker" "fuzzy-fur"}}
    "fuzzy-fur"
    {:cat/id "fuzzy-fur"
     :cat/name "Fuzzy Fur"
     :cat/owner "bob-moran"
     :animal/loves #{}}
    "wild-cat"
    {:cat/id "wild-cat"
     :cat/name "Wild Cat"
     :animal/loves #{}}}}
  )

(defn qctx-example
  "Constructs an example Query Context."
  []
  {:db db-example})

;; ------------------------------------------------------------------------------
;; Reads

(defn- basic-fr
  "Concise helper for defining field resolvers"
  ([field-name ref? many? doc]
   (basic-fr field-name ref? many? doc
     (if ref?
       (throw (ex-info "Cannot create compute function for entity-typed FR" {:d2q.field/name field-name}))
       (fn [_ obj _ _]
         (get obj field-name)))))
  ([field-name ref? many? doc compute]
   {:d2q.field/name field-name
    :doc doc
    :d2q.field/ref? ref?
    :d2q.field/cardinality (if many? :d2q.field.cardinality/many :d2q.field.cardinality/one)
    :bs.d2q.field/acl [:everyone-can-read]
    :bs.d2q.field/compute
    compute}))

(def field-resolvers
  [{:d2q.field/name :find-person-by-id
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/one
    :bs.d2q.field/acl [:everyone-can-read]
    :bs.d2q.field/compute
    (fn [qctx obj _ [person-id]]
      (when-let [p (get-in (:db qctx) [:db/persons person-id])]
        p))}
   {:d2q.field/name :find-cat-by-id
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/one
    :bs.d2q.field/acl [:everyone-can-read]
    :bs.d2q.field/compute
    (fn [qctx obj _ [cat-id]]
      (when-let [c (get-in (:db qctx) [:db/cats cat-id])]
        c))}
   {:d2q.field/name :animal/loves
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/many
    :bs.d2q.field/acl [:everyone-can-read]
    :bs.d2q.field/compute
    (fn [qctx obj _ _]
      (let [{cats :db/cats persons :db/persons} (:db qctx)]
        (->> obj :animal/loves set
          (mapv (fn [id]
                  (or
                    (get cats id)
                    (get persons id)))))
        ))}
   {:d2q.field/name :animal/loved-by
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/many
    :bs.d2q.field/acl [:everyone-can-read]
    :bs.d2q.field/compute
    (fn [qctx obj _ _]
      (let [{cats :db/cats persons :db/persons} (:db qctx)
            id (or (:person/id obj) (:cat/id obj))]
        (->> (concat (vals cats) (vals persons))
          (filter (fn [a]
                    (contains? (or (:animal/loves a) #{}) id)))
          vec)
        ))}
   {:d2q.field/name :find-all-humans
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/many
    :bs.d2q.field/acl [:everyone-can-read]
    :bs.d2q.field/compute
    (fn [{:keys [db]} _ _ _]
      (->> db :db/persons vals))}
   {:d2q.field/name :find-all-cats
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/many
    :bs.d2q.field/acl [:everyone-can-read]
    :bs.d2q.field/compute
    (fn [{:keys [db]} _ _ _]
      (->> db :db/cats vals))}
   {:d2q.field/name :find-persons-with-gender
    :doc "Example of a cardinality-many entity-typed FR with params"
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/many
    :bs.d2q.field/acl [:everyone-can-read]
    :bs.d2q.field/compute
    (fn [{:keys [db]} _ _ [gender]]
      (->> db :db/persons vals
        (filter #(-> % :person/gender (= gender)))))}

   (basic-fr :person/id false false "UID of a person")
   (basic-fr :person/email false false "Email of a person")
   (basic-fr :person/gender false false "Gender, an enum-valued FR, optional")
   (basic-fr :person/age false false "Age, optional")
   (basic-fr :person/address false false "Example of a map-valued scalar field")
   (basic-fr :person/notes false false "Example of a list-valued scalar field")
   (basic-fr :cat/id false false "")
   (basic-fr :cat/name false false "")
   (basic-fr :cat/owner true false "A to-one entity-typed field"
     (fn [{:keys [db]} obj _ _]
       (get-in db [:db/persons (:cat/owner obj)])))

   (basic-fr :one-nil true false "A to-one FR returning nil"
     (constantly nil))
   (basic-fr :many-nil true true "A to-many FR returning nil"
     (constantly nil))
   (basic-fr :scalar-nil false false "A scalar FR returning nil"
     (constantly nil))

   (basic-fr :scalar-throws false false ""
     (fn [_ _ _ _] (throw (ex-info "Scalar failed" {:error-data 42}))))
   (basic-fr :one-throws true false ""
     (fn [_ _ _ _] (throw (ex-info "To-one failed" {:error-data 42}))))
   (basic-fr :many-throws true false ""
     (fn [_ _ _ _] (throw (ex-info "To-many failed" {:error-data 42}))))
   ])

(defn engine2
  [field-resolvers]
  (d2q.api/query-engine
    [(d2q.api/tabular-resolver-from-field-resolvers ::default field-resolvers)]
    (map #(assoc % :d2q.field/resolver ::default) field-resolvers)
    (fn [qctx query o+is] o+is)))



(defn query-engine-example
  []
  (fn [qctx query obj]
    ;;  Execution time mean : 228.790078 µs on first sample query
    @((engine2 field-resolvers)
       qctx (d2q/normalize-query query) obj))
  ;; Execution time mean : 48.560712 µs on first sample query
  #_(d2q/engine field-resolvers
      {:demand-driven.authorization.read/accesses-for-object (constantly #{:everyone-can-read})}))

(defn q
  "Runs a query on the example dataset"
  [query]
  (let [q-engine (query-engine-example)
        qctx (qctx-example)
        root-obj {}]
    (q-engine qctx query root-obj)))

(defn fcall
  "Helper for writing Field Calls concisely"
  ([field-name nested]
   {:d2q.fcall/field field-name
    :d2q.fcall/nested nested})
  ([field-name key args]
   {:d2q.fcall/key key
    :d2q.fcall/field field-name
    :d2q.fcall/args args})
  ([field-name key args nested]
   {:d2q.fcall/key key
    :d2q.fcall/field field-name
    :d2q.fcall/args args
    :d2q.fcall/nested nested}))

(fact "Canonical example"
  (q (let [human-q [:person/id :person/email :person/age :person/address
                    (fcall :animal/loves
                      [:person/id :cat/id])]]
       [(fcall :find-person-by-id "jd" ["john-doe"]
          human-q)
        (fcall :find-all-humans "humans" nil
          human-q)
        (fcall :find-all-cats "m" nil
          [:cat/id :cat/name
           {:d2q.fcall/field :cat/owner
            :d2q.fcall/nested
            [:person/email]}])]))
  =>
  {"jd" {:person/id "john-doe",
         :person/email "john.doe@gmail.com",
         :person/age 18,
         :person/address {:address/number "48", :address/street "rue de Rome", :address/city "Paris"},
         :animal/loves [{:person/id "alice-hacker"} {:cat/id "minou"}]},
   "humans" [{:person/id "john-doe",
              :person/email "john.doe@gmail.com",
              :person/age 18,
              :person/address {:address/number "48", :address/street "rue de Rome", :address/city "Paris"},
              :animal/loves [{:person/id "alice-hacker"} {:cat/id "minou"}]}
             {:person/id "alice-hacker", :person/email "alice.hacker@gmail.com", :animal/loves []}
             {:person/id "bob-moran",
              :person/email "bob.moran@gmail.com",
              :person/address {:address/number "17", :address/street "rue de Mars", :address/city "Orléans"},
              :animal/loves []}],
   "m" [{:cat/id "minou", :cat/name "Minou", :cat/owner {:person/email "john.doe@gmail.com"}}
        {:cat/id "fuzzy-fur", :cat/name "Fuzzy Fur", :cat/owner {:person/email "bob.moran@gmail.com"}}
        {:cat/id "wild-cat", :cat/name "Wild Cat"}]}
  )

(fact "When entity does not exist, not added to the result"
  (q [{:d2q.fcall/key "x"
       :d2q.fcall/field :find-person-by-id
       :d2q.fcall/args ["does not exist"]
       :d2q.fcall/nested
       [:person/id
        {:d2q.fcall/field :animal/loves
         :d2q.fcall/nested
         [:person/id :cat/id]}]}])
  => {}

  (q [{:d2q.fcall/key "w"
       :d2q.fcall/field :find-cat-by-id
       :d2q.fcall/args ["wild-cat"]
       :d2q.fcall/nested
       [:cat/id
        {:d2q.fcall/field :cat/owner
         :d2q.fcall/nested
         [:person/id]}]}])
  => {"w" {:cat/id "wild-cat"}}
  )

(fact "When a Field Resolver returns nil, the key is not added to the result."
  (q [:many-nil :one-nil :scalar-nil])
  => {}

  (fact "When an Entity-typed Field Resolver returns nil, the nested fields are not computed"
    (q [(fcall :one-nil
          [:scalar-throws :one-throws :many-throws])])
    => {}))

(fact "When a Field Resolver throws, the whole query fails, with informative data about the error."
  (tabular
    (fact
      (try
        (q [(fcall ?field-name ?field-name "aaaaaargs")])
        :should-have-failed
        (catch Throwable err
          (ex-data err)))
      =>
      (contains
        {:q-field {:d2q.fcall/field ?field-name,
                   :d2q.fcall/key ?field-name
                   :d2q.fcall/args "aaaaaargs"},
         :d2q.error/type :d2q.error.type/field-resolver-failed-to-compute
         :error-data 42})
      )
    ?field-name
    :scalar-throws
    :one-throws
    :many-throws
    ))

;; TODO tests for authorization. (Val, 20 Nov 2017)










