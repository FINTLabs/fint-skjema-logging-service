pipeline {
    agent { label 'docker' }
    stages {
        stage('Build') {
            steps {
                withDockerRegistry([credentialsId: 'fintlabsacr.azurecr.io', url: 'https://fintlabsacr.azurecr.io']) {
                    sh "docker build --tag ${GIT_COMMIT} ."
                }
            }
        }
        stage('Publish') {
            when { branch 'master' }
            steps {
                sh "docker tag ${GIT_COMMIT} fintlabsacr.azurecr.io/fint-skjema-logging-service:build.${BUILD_NUMBER}_${GIT_COMMIT}"
                withDockerRegistry([credentialsId: 'fintlabsacr.azurecr.io', url: 'https://fintlabsacr.azurecr.io']) {
                    sh "docker push fintlabsacr.azurecr.io/fint-skjema-logging-service:build.${BUILD_NUMBER}_${GIT_COMMIT}"
                }
                /**
                 * Uncomment to auto deploy to your prefered environment
                 */
                //kubernetesDeploy configs: 'k8s.yaml', kubeconfigId: 'aks-alpha-fint'
                //kubernetesDeploy configs: 'k8s.yaml', kubeconfigId: 'aks-beta-fint'
                //kubernetesDeploy configs: 'k8s.yaml', kubeconfigId: 'aks-api-fint'

            }
        }
/**
 * Uncomment if you like to publish a PR or a version.
 */
//        stage('Publish PR') {
//            when { changeRequest() }
//            steps {
//                withDockerRegistry([credentialsId: 'fintlabsacr.azurecr.io', url: 'https://fintlabsacr.azurecr.io']) {
//                    sh "docker tag ${GIT_COMMIT} fintlabsacr.azurecr.io/fint-skjema-logging-service:${BRANCH_NAME}.${BUILD_NUMBER}"
//                    sh "docker push fintlabsacr.azurecr.io/fint-skjema-logging-service:${BRANCH_NAME}.${BUILD_NUMBER}"
//                }
//            }
//        }
//        stage('Publish Version') {
//            when {
//                tag pattern: "v\\d+\\.\\d+\\.\\d+(-\\w+-\\d+)?", comparator: "REGEXP"
//            }
//            steps {
//                script {
//                    VERSION = TAG_NAME[1..-1]
//                }
//                sh "docker tag ${GIT_COMMIT} fintlabsacr.azurecr.io/fint-skjema-logging-service:${VERSION}"
//                withDockerRegistry([credentialsId: 'fintlabsacr.azurecr.io', url: 'https://fintlabsacr.azurecr.io']) {
//                    sh "docker push fintlabsacr.azurecr.io/fint-skjema-logging-service:${VERSION}"
//                }
//            }
//        }
    }
}