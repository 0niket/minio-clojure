(ns minio-clojure.utils)

(defn parse-bool
  [val]
  (boolean (Boolean/valueOf val)))

(defn parse-int
  [val]
  (Integer/parseInt val))