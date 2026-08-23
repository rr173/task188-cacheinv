#!/bin/bash
# usage: bash build_benzhi_docker.sh <镜像名> <平台>
set -e

IMAGE_NAME=${1:-my-project}
DOCKER_PLATFORM=${2:-linux/amd64}

docker build --platform "$DOCKER_PLATFORM" -f benzhi.Dockerfile -t "$IMAGE_NAME" .

echo ""
echo "✅ Docker image '$IMAGE_NAME' built successfully!"
echo ""
echo "📋 Next steps (for testing):"
echo "  • Interactive shell：docker run -it $IMAGE_NAME:latest"
echo "  • Smoke test：docker run --rm $IMAGE_NAME:latest --smoke-test"
