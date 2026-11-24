# Laboratorio 11 - Infraestructura Big Data como Código

## Universidad César Vallejo
Curso: Business Intelligence and Big Data

## Objetivo
Crear una infraestructura Big Data local mediante Terraform y Floci.

## Herramientas
- Docker Desktop
- Terraform
- AWS CLI
- Git
- Floci

## Arquitectura
- Amazon S3: Data Lake principal.
- AWS Glue: catálogo de datos.
- Terraform: aprovisionamiento de infraestructura.
- Floci: emulación local de AWS.

## Data Lake
Bucket: ucv-data-platform

Dominios:
- sales
- customers
- products
- marketing

Capas:
- Bronze
- Silver
- Gold

## Endpoint
http://localhost:4566

## Comandos principales
```powershell
docker compose up -d
cd terraform
terraform init
terraform fmt
terraform validate
terraform plan
terraform apply
terraform state list
