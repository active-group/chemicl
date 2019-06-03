(defproject chemicl "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [de.active-group/active-clojure "0.28.0"]]

  :profiles {:dev {:resource-paths ["resources-test"]
                   :dependencies [[org.clojure/core.async "0.4.490"]]}}

  :test-selectors {:default (complement :slow)
                   :all (constantly true)}
  )
