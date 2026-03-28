## Ray Setup Guide for Feast

This guide provides step-by-step instructions to set up a Ray cluster on AWS EKS for use with Feast's Ray compute engine. Follow each section carefully to ensure a successful deployment.

---

### 1. Prerequisites

- [Homebrew](https://brew.sh/)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- [eksctl](https://eksctl.io/)
- [Helm](https://helm.sh/)

Install required tools (if not already installed):

```bash
brew install aws/tap/eksctl helm
```

---

### 2. Create EKS Cluster

```bash
eksctl create cluster \
  --name feast-ray-cluster \
  --region us-west-2 \
  --without-nodegroup
```

---

### 3. Install KubeRay Operator

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator
```

---

### 4. Configure IAM for S3 Access

Add IAM OIDC provider:

```bash
eksctl utils associate-iam-oidc-provider \
  --region us-west-2 \
  --cluster feast-ray-cluster \
  --approve
```

Create an S3 access policy (edit `<MY-BUCKET>` as needed):

```bash
aws iam create-policy \
  --policy-name FeastRayClusterS3ScopedPolicy \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "ReadTrainingData",
        "Effect": "Allow",
        "Action": [
          "s3:GetObject"
        ],
        "Resource": [
          "arn:aws:s3:::<MY-BUCKET>/criteo/*"
        ]
      },
      {
        "Sid": "WriteCheckpoints",
        "Effect": "Allow",
        "Action": [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ],
        "Resource": [
          "arn:aws:s3:::<MY-BUCKET>/feast/*"
        ]
      },
      {
        "Sid": "ListBucket",
        "Effect": "Allow",
        "Action": "s3:ListBucket",
        "Resource": "arn:aws:s3:::<MY-BUCKET>"
      }
    ]
  }'
```

Get your AWS account ID:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
```

Create a Kubernetes service account linked to the policy:

```bash
eksctl create iamserviceaccount \
  --name ray-s3-sa \
  --namespace default \
  --cluster feast-ray-cluster \
  --region us-west-2 \
  --attach-policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/FeastRayClusterS3ScopedPolicy \
  --approve
```

---

### 5. Create Node Groups

**System Node Group:**

```bash
eksctl create nodegroup \
  --cluster feast-ray-cluster \
  --name ray-system-ng \
  --node-type t3.small \
  --region us-west-2 \
  --nodes 1 --nodes-min 1 --nodes-max 1 \
  --node-labels role=system
```

---

### 6. Enable Cluster Autoscaler

Create the autoscaler policy (see [AWS docs](https://docs.aws.amazon.com/eks/latest/best-practices/cas.html)):

```bash
aws iam create-policy \
  --policy-name ClusterAutoscalerPolicy \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:DescribeAutoScalingInstances",
          "autoscaling:DescribeLaunchConfigurations",
          "autoscaling:DescribeScalingActivities",
          "autoscaling:DescribeTags",
          "ec2:DescribeImages",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeLaunchTemplateVersions",
          "ec2:GetInstanceTypesFromInstanceRequirements",
          "eks:DescribeNodegroup"
        ],
        "Resource": ["*"]
      },
      {
        "Effect": "Allow",
        "Action": [
          "autoscaling:SetDesiredCapacity",
          "autoscaling:TerminateInstanceInAutoScalingGroup"
        ],
        "Resource": ["*"]
      }
    ]
  }'
```

Link the policy to a service account:

```bash
eksctl create iamserviceaccount \
  --name cluster-autoscaler \
  --namespace kube-system \
  --cluster feast-ray-cluster \
  --region us-west-2 \
  --attach-policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/ClusterAutoscalerPolicy \
  --approve \
  --override-existing-serviceaccounts
```

Install the cluster autoscaler:

```bash
helm repo add autoscaler https://kubernetes.github.io/autoscaler
helm install cluster-autoscaler autoscaler/cluster-autoscaler \
  --namespace kube-system \
  --set autoDiscovery.clusterName=feast-ray-cluster \
  --set awsRegion=us-west-2 \
  --set rbac.serviceAccount.create=false \
  --set rbac.serviceAccount.name=cluster-autoscaler
```

---

### 7. Add Ray Node Groups

**Head Node Group (tainted):**

```bash
eksctl create nodegroup \
  --cluster feast-ray-cluster \
  --name ray-head-ng \
  --node-type m5.large \
  --region us-west-2 \
  --nodes 1 --nodes-min 1 --nodes-max 1 \
  --node-labels role=ray-head
```

Taint the head node, worker processes not allowed on the head node:

```bash
kubectl taint nodes -l role=ray-head role=ray-head:NoSchedule
```

**Worker Node Group:**

```bash
eksctl create nodegroup \
  --cluster feast-ray-cluster \
  --name ray-ng \
  --node-type m5.xlarge \
  --region us-west-2 \
  --nodes 2 --nodes-min 2 --nodes-max 4 \
  --node-labels role=ray-worker
```

---

### 8. Deploy Ray Cluster

```bash
kubectl apply -f rayCluster.yaml
```

---

### 9. Access Ray Dashboard & Submit Jobs

Forward Ray cluster ports to localhost:

```bash
kubectl port-forward svc/ray-compute-engine-head-svc 10001:10001
kubectl port-forward svc/ray-compute-engine-head-svc 8265:8265
```

Submit a Ray job (example):

```bash
ray job submit \
  --address http://localhost:8265 \
  --working-dir . \
  --runtime-env-json '{
    "pip": ["polars", "joblib", "scikit-learn"],
    "env_vars": {
      "WINDOW_DAYS": "2",
      "END_DATE": "2026-03-20"
    },
    "excludes": ["rayCluster.yaml", "README.md"]
  }' \
  -- python ray_job.py
```

---

### 10. Delete the Cluster

```bash
eksctl delete cluster \
  --name feast-ray-cluster \
  --region us-west-2
```

---

## References

- [Ray on KubeRay](https://docs.ray.io/en/latest/cluster/kubernetes/index.html)
- [Feast Documentation](https://docs.feast.dev/)
- [AWS EKS Best Practices](https://aws.github.io/aws-eks-best-practices/)
