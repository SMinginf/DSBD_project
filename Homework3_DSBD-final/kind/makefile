# Makefile

KUBECTL = kubectl
MANIFEST_DIR = ./manifests
NAMESPACE = my-app 


# Comando per creare i namespace
apply-namespaces:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/namespaces.yaml
	
# Comando per creare i vari secret 
apply-secrets:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/secrets.yaml -n $(NAMESPACE)
		

apply-mysql:
	#kind load docker-image mysql --name mio-cluster
	$(KUBECTL) apply -f $(MANIFEST_DIR)/mysql-cm1-configmap.yaml -n $(NAMESPACE)
	$(KUBECTL) apply -f $(MANIFEST_DIR)/mysql-deployment.yaml -n $(NAMESPACE)
	
apply-redis:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/redis-statefulset.yaml -n $(NAMESPACE)

apply-kafka:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/kafka-deployment.yaml -n $(NAMESPACE)
	
apply-prometheus:
	helm install prometheus prometheus-community/kube-prometheus-stack -n monitoring
	$(KUBECTL) apply -f $(MANIFEST_DIR)/serviceMonitor.yaml -n monitoring
	
apply-grpc_server:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/grpc-server-deployment.yaml -n $(NAMESPACE)
	
apply-data_collector:	
	$(KUBECTL) apply -f $(MANIFEST_DIR)/data-collector-deployment.yaml -n $(NAMESPACE)
	
apply-alert_system:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/alert-system-deployment.yaml -n $(NAMESPACE)
	
apply-notifier:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/alert-notifier-system-deployment.yaml -n $(NAMESPACE)

apply-all:
	$(KUBECTL) apply -f $(MANIFEST_DIR)/namespaces.yaml
	$(KUBECTL) apply -f $(MANIFEST_DIR)/secrets.yaml -n $(NAMESPACE)
	helm install prometheus prometheus-community/kube-prometheus-stack -n monitoring
	$(KUBECTL) apply -f $(MANIFEST_DIR)/mysql-cm1-configmap.yaml -n $(NAMESPACE)
	$(KUBECTL) apply -f $(MANIFEST_DIR)/mysql-deployment.yaml -n $(NAMESPACE)
	$(KUBECTL) apply -f $(MANIFEST_DIR)/redis-statefulset.yaml -n $(NAMESPACE)
	$(KUBECTL) apply -f $(MANIFEST_DIR)/kafka-deployment.yaml -n $(NAMESPACE)
	$(KUBECTL) apply -f $(MANIFEST_DIR)/serviceMonitor.yaml -n monitoring
	$(KUBECTL) apply -f $(MANIFEST_DIR)/grpc-server-deployment.yaml -n $(NAMESPACE)
	$(KUBECTL) apply -f $(MANIFEST_DIR)/data-collector-deployment.yaml -n $(NAMESPACE)
	$(KUBECTL) apply -f $(MANIFEST_DIR)/alert-system-deployment.yaml -n $(NAMESPACE)
	$(KUBECTL) apply -f $(MANIFEST_DIR)/alert-notifier-system-deployment.yaml -n $(NAMESPACE)
		
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


delete-redis:
	$(KUBECTL) delete statefulset redis-statefulset -n $(NAMESPACE)
	
delete-alert_system:
	$(KUBECTL) delete deployment alert-system-deployment -n $(NAMESPACE)
	
# Comando per rimuovere tutte le risorse
delete-all:
	$(KUBECTL) delete -f $(MANIFEST_DIR) -n $(NAMESPACE)


