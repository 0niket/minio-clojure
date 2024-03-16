(ns minio-clojure.core
  (:require [environ.core :refer [env]]
            [minio-clojure.utils :as utils]
            [clojure.string :as str])
  (:import [io.minio MinioClient]
           [io.minio MakeBucketArgs]
           [io.minio RemoveBucketArgs]
           [io.minio BucketExistsArgs]
           [io.minio RemoveObjectArgs]
           [io.minio GetObjectArgs]
           [io.minio UploadObjectArgs]))

(def conn-opts
  {:endpoint {:host (env :minio-endpoint-host)
              :port (utils/parse-int (env :minio-endpoint-port))
              :tls? (utils/parse-bool (env :minio-endpoint-tls-enabled))}
   :creds {:access-key (env :minio-creds-access-key)
           :secret-key (env :minio-creds-secret-key)}})
(def bucket-name (env :minio-bucket))

(def base-url-local (format "http://%s:%s"
                            (get-in conn-opts [:endpoint :host])
                            (get-in conn-opts [:endpoint :port])))
(def base-url-prod "")

(def base-url (if (= "local" (env :profile))
                base-url-local
                base-url-prod))


(defn build-connection
  "Build connection to minio"
  [{:keys [host port tls?] :as _endpoint} {:keys [access-key secret-key] :as _creds}]
  (-> (MinioClient/builder)
      (.endpoint host port tls?)
      (.credentials access-key secret-key)
      (.build)))

(defn bucket-exists?
  "Predicate to check existance of bucket"
  [conn name]
  (let [bucket (-> (BucketExistsArgs/builder)
                   (.bucket name)
                   (.build))]
    (.bucketExists conn bucket)))

(defn make-bucket
  [conn name]
  (let [bucket (-> (MakeBucketArgs/builder)
                   (.bucket name)
                   (.build))]
    (.makeBucket conn bucket)))

(defn remove-bucket
  [conn name]
  (let [bucket (-> (RemoveBucketArgs/builder)
                   (.bucket name)
                   (.build))]
    (.removeBucket conn bucket)))

(defn list-buckets
  [conn]
  (.listBuckets conn))

(defn list-bucket-names
  [conn]
  (map #(.name %) (list-buckets conn)))

(defn upload-object
  [conn user-id {:keys [filename tempfile]}]
  {:pre [(bucket-exists? conn bucket-name)]}
  (let [ms (System/currentTimeMillis)
        filename (str/replace filename #"\s+" "_")
        filename (format "%s_%s_%s" user-id ms filename)
        object-url (format "%s/%s/%s" base-url bucket-name filename)
        upload-object-args (-> (UploadObjectArgs/builder)
                               (.bucket bucket-name)
                               (.object filename)
                               (.filename (.getPath tempfile))
                               (.build))]
    (.uploadObject conn upload-object-args)
    object-url))

(defn remove-object
  [conn filename]
  {:pre [(bucket-exists? conn bucket-name)]}
  (let [remove-object-args (-> (RemoveObjectArgs/builder)
                               (.bucket bucket-name)
                               (.object filename)
                               (.build))]
    (.removeObject conn remove-object-args)))

(defn get-object
  [conn filename]
  {:pre [(bucket-exists? conn bucket-name)]}
  (let [get-object-args (-> (GetObjectArgs/builder)
                            (.bucket bucket-name)
                            (.object filename)
                            (.build))]
    (-> conn
        (.getObject get-object-args)
        (.object))))

(defn object-exists?
  [conn filename]
  {:pre [(bucket-exists? conn bucket-name)]}
  (string? (get-object conn filename)))

(comment
  (def conn* (build-connection (:endpoint conn-opts) (:creds conn-opts)))
  (upload-object conn*
                 (random-uuid)
                 {:filename "foo.md" :tempfile (clojure.java.io/file "README.md")})
  (get-object conn* "03fbb1ba-e3d4-43d8-a8f1-17139b517ae6_1707129476276_foo.md")
  (object-exists? conn* "README.md")
  (remove-object conn* "03fbb1ba-e3d4-43d8-a8f1-17139b517ae6_1707129476276_foo.md")
  (make-bucket conn* "bazar-test")
  (remove-bucket conn* "bazar-test"))