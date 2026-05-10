package controller

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dangoodman/k8s-sleepy-proxy/internal/sleepy"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type TenantVolumeConfig struct {
	Enabled          bool
	ClaimName        string
	MountPath        string
	StorageClassName string
	Size             string
	ProvisionTimeout time.Duration
}

type KubernetesManager struct {
	client        kubernetes.Interface
	namespace     string
	sidecarImage  string
	controllerURL string
	secretName    string
	volume        TenantVolumeConfig
}

func NewKubernetesManager(namespace, sidecarImage, controllerURL, secretName string, volume TenantVolumeConfig) (*KubernetesManager, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			return nil, err
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &KubernetesManager{
		client:        client,
		namespace:     namespace,
		sidecarImage:  sidecarImage,
		controllerURL: controllerURL,
		secretName:    secretName,
		volume:        volume.withDefaults(),
	}, nil
}

func (v TenantVolumeConfig) withDefaults() TenantVolumeConfig {
	if v.ClaimName == "" {
		v.ClaimName = "data"
	}
	if v.MountPath == "" {
		v.MountPath = "/data"
	}
	if v.StorageClassName == "" {
		v.StorageClassName = "archil"
	}
	if v.Size == "" {
		v.Size = "1Gi"
	}
	if v.ProvisionTimeout == 0 {
		v.ProvisionTimeout = 60 * time.Second
	}
	return v
}

func (v TenantVolumeConfig) pvcName(tenantID string) string {
	return fmt.Sprintf("%s-%s-0", v.ClaimName, sleepy.WorkloadName(tenantID))
}

func (m *KubernetesManager) EnsureTenantVolume(ctx context.Context, t sleepy.Tenant) error {
	if !m.volume.Enabled {
		return nil
	}
	pvc := m.tenantPVC(t)
	existing, err := m.client.CoreV1().PersistentVolumeClaims(m.namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := m.client.CoreV1().PersistentVolumeClaims(m.namespace).Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		if existing.Status.Phase == corev1.ClaimBound {
			return nil
		}
	}
	return m.waitPVCBound(ctx, pvc.Name, m.volume.ProvisionTimeout)
}

func (m *KubernetesManager) EnsureTenant(ctx context.Context, t sleepy.Tenant) (string, error) {
	if err := m.EnsureTenantVolume(ctx, t); err != nil {
		return "", err
	}

	name := sleepy.WorkloadName(t.TenantID)
	labels := sleepy.TenantLabels(t.TenantID)
	backend := fmt.Sprintf("%s.%s.svc.cluster.local:80", name, m.namespace)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       80,
				TargetPort: intstr.FromInt(8080),
			}},
		},
	}
	existingSvc, err := m.client.CoreV1().Services(m.namespace).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := m.client.CoreV1().Services(m.namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
			return "", err
		}
	case err != nil:
		return "", err
	default:
		svc.ResourceVersion = existingSvc.ResourceVersion
		svc.Spec.ClusterIP = existingSvc.Spec.ClusterIP
		svc.Spec.ClusterIPs = existingSvc.Spec.ClusterIPs
		svc.Spec.IPFamilies = existingSvc.Spec.IPFamilies
		svc.Spec.IPFamilyPolicy = existingSvc.Spec.IPFamilyPolicy
		if _, err := m.client.CoreV1().Services(m.namespace).Update(ctx, svc, metav1.UpdateOptions{}); err != nil {
			return "", err
		}
	}

	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: name,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: []corev1.LocalObjectReference{{
						Name: "docr-pull-secret",
					}},
					Containers: []corev1.Container{
						{
							Name:  "app",
							Image: t.Image,
							Env: []corev1.EnvVar{{
								Name:  "DATA_DIR",
								Value: m.volume.MountPath,
							}},
							VolumeMounts: m.tenantVolumeMounts(),
							Ports: []corev1.ContainerPort{{
								Name:          "app",
								ContainerPort: int32(t.UpstreamPort),
							}},
						},
						{
							Name:  "sleepy-sidecar",
							Image: m.sidecarImage,
							Env: []corev1.EnvVar{
								{Name: "TENANT_ID", Value: t.TenantID},
								{Name: "UPSTREAM_PORT", Value: fmt.Sprintf("%d", t.UpstreamPort)},
								{Name: "IDLE_SECONDS", Value: fmt.Sprintf("%d", t.IdleSeconds)},
								{Name: "CONTROLLER_URL", Value: m.controllerURL},
								{
									Name: "AUTH_TOKEN",
									ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: m.secretName},
										Key:                  "auth-token",
									}},
								},
							},
							Ports: []corev1.ContainerPort{{
								Name:          "http",
								ContainerPort: 8080,
							}},
						},
					},
				},
			},
			VolumeClaimTemplates: m.tenantVolumeClaimTemplates(t),
		},
	}

	existingSts, err := m.client.AppsV1().StatefulSets(m.namespace).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := m.client.AppsV1().StatefulSets(m.namespace).Create(ctx, sts, metav1.CreateOptions{}); err != nil {
			return "", err
		}
	case err != nil:
		return "", err
	default:
		sts.ResourceVersion = existingSts.ResourceVersion
		if _, err := m.client.AppsV1().StatefulSets(m.namespace).Update(ctx, sts, metav1.UpdateOptions{}); err != nil {
			return "", err
		}
	}

	return backend, nil
}

func (m *KubernetesManager) tenantPVC(t sleepy.Tenant) *corev1.PersistentVolumeClaim {
	storageClass := m.volume.StorageClassName
	labels := sleepy.TenantLabels(t.TenantID)
	labels["app.kubernetes.io/component"] = "tenant-volume"
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.volume.pvcName(t.TenantID),
			Namespace: m.namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(m.volume.Size),
				},
			},
		},
	}
}

func (m *KubernetesManager) tenantVolumeMounts() []corev1.VolumeMount {
	if !m.volume.Enabled {
		return nil
	}
	return []corev1.VolumeMount{{
		Name:      m.volume.ClaimName,
		MountPath: m.volume.MountPath,
	}}
}

func (m *KubernetesManager) tenantVolumeClaimTemplates(t sleepy.Tenant) []corev1.PersistentVolumeClaim {
	if !m.volume.Enabled {
		return nil
	}
	pvc := m.tenantPVC(t)
	pvc.Name = m.volume.ClaimName
	pvc.Namespace = ""
	return []corev1.PersistentVolumeClaim{*pvc}
}

func (m *KubernetesManager) waitPVCBound(ctx context.Context, name string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		pvc, err := m.client.CoreV1().PersistentVolumeClaims(m.namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pvc.Status.Phase == corev1.ClaimBound {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("tenant volume claim %s did not bind before timeout", name)
		case <-ticker.C:
		}
	}
}

func (m *KubernetesManager) WaitReady(ctx context.Context, tenantID string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		pods, err := m.client.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labelsSelector(tenantID),
		})
		if err != nil {
			return err
		}
		for _, pod := range pods.Items {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("tenant %s did not become ready before timeout", tenantID)
		case <-ticker.C:
		}
	}
}

func (m *KubernetesManager) DeleteTenant(ctx context.Context, tenantID string) error {
	name := sleepy.WorkloadName(tenantID)
	propagation := metav1.DeletePropagationBackground
	err := m.client.AppsV1().StatefulSets(m.namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	err = m.client.CoreV1().Services(m.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func labelsSelector(tenantID string) string {
	return "sleepy.dev/managed-by=sleepy-controller,sleepy.dev/tenant-id=" + tenantID
}
