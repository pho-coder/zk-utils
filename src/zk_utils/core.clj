(ns zk-utils.core
  (:require [zookeeper :as zk])
  (:gen-class))

(defn positive-numbers 
    ([] (positive-numbers 1))
    ([n] (cons n (lazy-seq (positive-numbers (inc n))))))

(defn get-one-data-list [prefix num-list]
  (map #(condp > %
          10 (str prefix "000000" %)
          100 (str prefix "00000" %)
          1000 (str prefix "0000" %)
          10000 (str prefix "000" %)
          100000 (str prefix "00" %)
          1000000 (str prefix "0" %)
          10000000 (str prefix %)
          (str %)) num-list))

(defn insert-data-list [zk-address zk-root data-list]
  (let [client (zk/connect zk-address)
        start-time (System/currentTimeMillis)
        total-count (ref 0)
        exist-count (ref 0)]
    (dorun (map #(let [rs (zk/create client (str zk-root %) :persistent? true :data (.getBytes %))]
                   (if (= rs false)
                     (dosync
                      (alter exist-count inc))
                     (dosync
                      (alter total-count inc)))
                   (if (= (mod @total-count 5000) 0)
                     (prn @total-count))) data-list))
    (let [end-time (System/currentTimeMillis)
          total-time (- end-time start-time)]
      (zk/close client)
      {:total-time total-time
       :total-count @total-count
       :exist-count @exist-count
       :avg-time (quot total-time (+ @total-count @exist-count))})))

(defn get-children [zk-address zk-root]
  (let [client (zk/connect zk-address)
        start-time (System/currentTimeMillis)
        children (zk/children client zk-root)]
    (zk/close client)
    (prn "get clildren time:" (- (System/currentTimeMillis) start-time) " length: " (.size children))
    children))

(defn get-data [zk-address children]
  (let [client (zk/connect zk-address)
        start-time (System/currentTimeMillis)]
    (dorun (map #(zk/data client %) children))
    (zk/close client)
    (prn "get-data time:" (- (System/currentTimeMillis) start-time))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (prn args)
  (let [cmd (first args)
        zk-address (second args)
        zk-root (nth args 2)]
    (if (= cmd "create")
      (let [num (Integer/parseInt (nth args 3))
            thread-num (Integer/parseInt (nth args 4 "1"))
            running-jobs (loop [t-num thread-num a-list (list)]
                           (if (= t-num 0)
                             a-list
                             (recur (dec t-num)
                                    (cons (future (insert-data-list zk-address
                                                                    (str "/zk-test/" zk-root "/")
                                                                    (take num (get-one-data-list (str t-num) (positive-numbers)))))
                                          a-list))))]
        (prn running-jobs)
        (while (not (reduce #(and %1 (future-done? %2)) true running-jobs))
          (Thread/sleep 2000))
        (prn running-jobs)
        (dorun (map #(prn @%) running-jobs))
        (shutdown-agents))
      (if (= cmd "delete")
        (let [client (zk/connect zk-address)
              _ (zk/delete-all client (str "/zk-test/" zk-root))
              _ (zk/create client (str "/zk-test/" zk-root) :persistent? true :data (.getBytes zk-root))]
          (zk/close client))
        (if (= cmd "loop")
          (let [num (Integer/parseInt (nth args 3))]
            (while true
              (let [one-loop (future (insert-data-list zk-address
                                                       (str "/zk-test/" zk-root "/")
                                                       (take num (get-one-data-list "0" (positive-numbers)))))]
                (while (not (future-done? one-loop))
                  (Thread/sleep 1000))
                (prn @one-loop)
                (let [client (zk/connect zk-address)]
                  (zk/delete-all client (str "/zk-test/" zk-root))
                  (zk/create client (str "/zk-test/" zk-root) :persistent? true :data (.getBytes zk-root))
                  (zk/close client)))))
          (if (= cmd "read")
            (let [zk-path (str "/zk-test/" zk-root)
                  children (map #(str zk-path "/" %) (get-children zk-address zk-path))]
              (get-data zk-address children))))))))
