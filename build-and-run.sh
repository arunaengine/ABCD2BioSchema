#!/bin/bash

# GFBio Webhook Docker Build und Deploy Script

set -e

# Output Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}ABCD2BioSchema Service Docker Build und Deploy${NC}"

cleanup() {
    echo -e "${YELLOW}Cleanup...${NC}"
    docker compose down
}

build_image() {
    echo -e "${GREEN}Building Docker Image...${NC}"
    docker build -t abcd2bioschema:latest .
    echo -e "${GREEN}✓ Build run successfully!${NC}"
}

run_tests() {
    echo -e "${GREEN}Running Tests...${NC}"
    docker run --rm abcd2bioschema:latest cargo test
    echo -e "${GREEN}✓ Tests run successfully!${NC}"
}

deploy() {
    echo -e "${GREEN}Deploying Service...${NC}"
    docker compose up -d
    echo -e "${GREEN}✓ Service deployed!${NC}"
}

check_health() {
    echo -e "${GREEN}Checking Health...${NC}"
    sleep 10

    max_attempts=30
    attempt=1

    while [ $attempt -le $max_attempts ]; do
        if curl -f http://0.0.0.0:5000/health > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Service is running!${NC}"
            return 0
        fi

        echo -e "${YELLOW}Waiting for service... (Attempt $attempt/$max_attempts)${NC}"
        sleep 2
        ((attempt++))
    done

    echo -e "${RED}✗ Service is not available!${NC}"
    return 1
}

show_logs() {
    echo -e "${GREEN}Service Logs:${NC}"
    docker compose logs -f abcd_webhooker
}


case "${1:-deploy}" in
    "build")
        build_image
        ;;
    "test")
        build_image
        run_tests
        ;;
    "deploy")
        build_image
        deploy
        check_health
        ;;
    "logs")
        show_logs
        ;;
    "stop")
        cleanup
        ;;
    "restart")
        cleanup
        build_image
        deploy
        check_health
        ;;
    *)
        echo -e "${RED}Usage: $0 {build|test|deploy|logs|stop|restart}${NC}"
        echo ""
        echo "  build   - Build Docker Image"
        echo "  test    - Build und run Tests"
        echo "  deploy  - Build, Deploy und Health Check"
        echo "  logs    - Show Service Logs"
        echo "  stop    - Stop all Services"
        echo "  restart - Stop, Build und Deploy"
        exit 1
        ;;
esac

echo -e "${GREEN}Done!${NC}"

# Service Info
if [ "${1:-deploy}" = "deploy" ] || [ "${1}" = "restart" ]; then
    echo ""
    echo -e "${GREEN}📡 Service Endpoints:${NC}"
    echo "  Health Check: http://localhost:5000/health"
    echo "  Transform (Upload): POST http://localhost:5000/transform"
    echo "  Transform (URL): POST http://localhost:5000/transform/url"
    echo ""
    echo -e "${GREEN}🐳 Docker Commands:${NC}"
    echo "  Show Logs: docker-compose logs -f"
    echo "  Stop Service: docker-compose down"
    echo "  Restart Service: docker-compose restart"
fi