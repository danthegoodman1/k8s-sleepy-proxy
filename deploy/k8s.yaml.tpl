apiVersion: v1
kind: Namespace
metadata:
  name: sleepy-system
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sleepy-controller
  namespace: sleepy-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: sleepy-controller
  namespace: sleepy-system
rules:
  - apiGroups: [""]
    resources: ["services", "pods"]
    verbs: ["get", "list", "watch", "create", "update", "delete"]
  - apiGroups: ["apps"]
    resources: ["statefulsets"]
    verbs: ["get", "list", "watch", "create", "update", "delete"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: sleepy-controller
  namespace: sleepy-system
subjects:
  - kind: ServiceAccount
    name: sleepy-controller
roleRef:
  kind: Role
  name: sleepy-controller
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepy-controller
  namespace: sleepy-system
  labels:
    app.kubernetes.io/name: sleepy-controller
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepy-controller
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepy-controller
    spec:
      serviceAccountName: sleepy-controller
      imagePullSecrets:
        - name: docr-pull-secret
      containers:
        - name: controller
          image: __CONTROLLER_IMAGE__
          imagePullPolicy: Always
          ports:
            - containerPort: 8080
              name: http
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: sleepy-secrets
                  key: database-url
            - name: AUTH_TOKEN
              valueFrom:
                secretKeyRef:
                  name: sleepy-secrets
                  key: auth-token
            - name: SIDECAR_IMAGE
              value: __SIDECAR_IMAGE__
            - name: NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: SECRET_NAME
              value: sleepy-secrets
            - name: CONTROLLER_URL_FOR_SIDECAR
              value: http://sleepy-controller.sleepy-system.svc.cluster.local:8080
            - name: WAKE_TIMEOUT_SECONDS
              value: "120"
          readinessProbe:
            httpGet:
              path: /healthz
              port: http
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
---
apiVersion: v1
kind: Service
metadata:
  name: sleepy-controller
  namespace: sleepy-system
spec:
  selector:
    app.kubernetes.io/name: sleepy-controller
  ports:
    - name: http
      port: 8080
      targetPort: http
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepy-lb
  namespace: sleepy-system
  labels:
    app.kubernetes.io/name: sleepy-lb
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepy-lb
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepy-lb
    spec:
      imagePullSecrets:
        - name: docr-pull-secret
      containers:
        - name: lb
          image: __LB_IMAGE__
          imagePullPolicy: Always
          ports:
            - containerPort: 8080
              name: http
          env:
            - name: AUTH_TOKEN
              valueFrom:
                secretKeyRef:
                  name: sleepy-secrets
                  key: auth-token
            - name: CONTROLLER_URL
              value: http://sleepy-controller.sleepy-system.svc.cluster.local:8080
            - name: WAKE_TIMEOUT_SECONDS
              value: "120"
            - name: CACHE_TTL_SECONDS
              value: "5"
          readinessProbe:
            httpGet:
              path: /healthz
              port: http
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
---
apiVersion: v1
kind: Service
metadata:
  name: sleepy-lb
  namespace: sleepy-system
spec:
  type: LoadBalancer
  selector:
    app.kubernetes.io/name: sleepy-lb
  ports:
    - name: http
      port: 80
      targetPort: http
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: sleepy-image-cache
  namespace: sleepy-system
  labels:
    app.kubernetes.io/name: sleepy-image-cache
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepy-image-cache
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepy-image-cache
    spec:
      imagePullSecrets:
        - name: docr-pull-secret
      initContainers:
        - name: cache-controller
          image: __CONTROLLER_IMAGE__
          imagePullPolicy: Always
          command: ["/controller"]
          args: ["--cache-warm"]
        - name: cache-lb
          image: __LB_IMAGE__
          imagePullPolicy: Always
          command: ["/lb"]
          args: ["--cache-warm"]
        - name: cache-sidecar
          image: __SIDECAR_IMAGE__
          imagePullPolicy: Always
          command: ["/sidecar"]
          args: ["--cache-warm"]
        - name: cache-echo
          image: __ECHO_IMAGE__
          imagePullPolicy: Always
          command: ["/echo"]
          args: ["--cache-warm"]
      containers:
        - name: hold
          image: __ECHO_IMAGE__
          imagePullPolicy: IfNotPresent
          command: ["/echo"]
          env:
            - name: LISTEN_ADDR
              value: ":9000"
