--Entities where a contact has a title containing "finance"
	
	SELECT DISTINCT p.*
	FROM pursuit.places p
	JOIN pursuit.contacts c ON p.place_id = c.place_id
	WHERE LOWER(c.title) LIKE '%finance%';

	
--Contacts where an email contains "bob" and the entity has technology "Accela" and population > 10k:

	SELECT c.*
	FROM pursuit.contacts c
	JOIN pursuit.places p ON c.place_id = p.place_id
	JOIN pursuit.tech_stack t ON p.place_id = t.place_id
	WHERE LOWER(c.email) LIKE '%bob%'
	AND LOWER(t.name) = 'accela'
	AND p.population > 10000;

--entities that are synced w/ Customer A’s CRM.

  SELECT DISTINCT p.*
	FROM pursuit.places p
	JOIN pursuit.customer_place_relation cpr ON p.place_id = cpr.place_id
	JOIN pursuit.customer_mapping cm ON cpr.customer_id = cm.customer_id
	JOIN pursuit.customer c ON cm.customer_id = c.customer_id
	WHERE LOWER(c.name) = 'customer a'
	AND cm.last_synced_time >= CURRENT_DATE() - INTERVAL '30 days';

