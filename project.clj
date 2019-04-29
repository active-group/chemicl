(defproject chemicl "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [active-clojure "0.23.0"]]

  :profiles {:dev {:resource-paths ["resources-test"]}}

  :test-selectors {:default (complement :slow)
                   :all (constantly true)}
  )
