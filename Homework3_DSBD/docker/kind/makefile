# Makefile

KUBECTL = kubectl
MANIFEST_DIR = ./k8s_manifests
NAMESPACE = my-app  # Modifica con il tuo namespace, se necessario


# Comando per creare i namespace
apply-namespaces:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/namespace.yaml
	
# Comando per creare il secret con le credenziali del mio repository privato docker
apply-docker_secret:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/my-registry-secret.yaml -n $(NAMESPACE)
		
# Comando per aggiornare rispettivamente il file value.yaml di prometheus e prometheus-config.myaml
update-prometheus:
	#helm upgrade prometheus prometheus-community/kube-prometheus-stack -n monitoring -f values.yaml
	#kubectl apply -f prometheus-config.yaml


# Comando per applicare tutti i manifest per mysql
apply-mysql:
	#kind load docker-image mysql --name mio-cluster
	$(KUBECTL) apply -f $(MANIFEST_DIR)/mysql-cm1-configmap.yaml -n $(NAMESPACE)
	$(KUBECTL) apply -f $(MANIFEST_DIR)/mysql-deployment.yaml -n $(NAMESPACE)
	
	
apply-grpc_server:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/grpc-server-deployment.yaml -n $(NAMESPACE)
	
apply-data_collector:	
	$(KUBECTL) apply -f $(MANIFEST_DIR)/data-collector-deployment.yaml -n $(NAMESPACE)
	
apply-alert_system:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/alert-system-deployment.yaml -n $(NAMESPACE)
	
apply-notifier:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/alert-notifier-system-deployment.yaml -n $(NAMESPACE)
	
apply-kafka:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/kafka-deployment.yaml -n $(NAMESPACE)
	
apply-prometheus:
	helm install prometheus prometheus-community/kube-prometheus-stack -n monitoring
	$(KUBECTL) apply -f $(MANIFEST_DIR)/serviceMonitor.yaml -n monitoring
	
apply-redis:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/redis-statefulset.yaml -n $(NAMESPACE)
	
		
delete-mysql:
	$(KUBECTL) delete statefulset mysql-statefulset -n $(NAMESPACE)
	
delete-grpc_server:
	$(KUBECTL) delete deployment grpc-server-deployment -n $(NAMESPACE)
	
delete-notifier:
	$(KUBECTL) delete deployment alert-notifier-system-deployment -n $(NAMESPACE)
	
delete-kafka:
	$(KUBECTL) delete deployment kafka-deployment -n $(NAMESPACE)
	$(KUBECTL) delete deployment zookeeper-deployment -n $(NAMESPACE)
	
delete-prometheus:
	helm uninstall prometheus -n monitoring



# Comando per rimuovere tutte le risorse
delete:
	$(KUBECTL) delete -f $(MANIFEST_DIR) -n $(NAMESPACE)

# Comando per rimuovere manifest specifici
delete-specific:
	$(KUBECTL) delete -f $(MANIFEST_DIR)/<nome-del-manifest>.yaml -n $(NAMESPACE)

