(defproject bandsquare/d2q "0.0.1-SNAPSHOT"
  :description "An expressive toolkit for building efficient GraphQL-like query servers"
  :url "https://github.com/bandsquare/d2q"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [manifold "0.1.6"]]
  :profiles
  {:dev
   {:dependencies
    [[midje "1.7.0"]]}})
