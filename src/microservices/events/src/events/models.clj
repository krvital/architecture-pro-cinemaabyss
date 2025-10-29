(ns events.models)

(def MovieEvent
  [:map
   ["movie_id" int?]
   ["title" string?]
   ["action" string?]
   ["user_id" {:optional true} int?]
   ["rating" {:optional true} double?]
   ["genres" {:optional true} [:sequential string?]]
   ["description" {:optional true} string?]])

(def UserEvent
  [:map
   ["user_id" int?]
   ["timestamp" string?]
   ["action" string?]
   ["username" {:optional true} string?]
   ["email" {:optional true} string?]])

(def PaymentEvent
  [:map
   ["payment_id" int?]
   ["user_id" int?]
   ["amount" float?]
   ["status" string?]
   ["timestamp" string?]
   ["method_type" {:optional true} string?]])

