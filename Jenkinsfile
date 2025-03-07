#!groovy

// Global scope required for multi-stage persistence
def artifactoryStr = 'onsart-01'
artServer = Artifactory.server "${artifactoryStr}"
buildInfo = Artifactory.newBuildInfo()
def agentPython3Version = 'python_3.10'
def artifactVersion

// Define a function to push packaged code to Artifactory
def pushToPyPiArtifactoryRepo(String projectName, String sourceDist = 'dist/*', String artifactoryHost = 'onsart-01.ons.statistics.gov.uk') {
    withCredentials([usernamePassword(credentialsId: env.ARTIFACTORY_CREDS, usernameVariable: 'ARTIFACTORY_USER', passwordVariable: 'ARTIFACTORY_PASSWORD')]){ // pragma: allowlist secret
        sh "curl -u ${ARTIFACTORY_USER}:\${ARTIFACTORY_PASSWORD} -T ${sourceDist} 'http://${artifactoryHost}/artifactory/${env.ARTIFACTORY_PYPI_REPO}/${projectName}/'"
    }
}

// This section defines the Jenkins pipeline
pipeline {

    agent any

    libraries {
            // Useful library from ONS internal GitLab
        lib('jenkins-pipeline-shared@feature/dap-ci-scripts')
    }

    // Define environment variables
    environment {
        PROJECT_NAME = "mbs_results"
        MAIN_BRANCH = "main"
        PROXY = credentials("PROXY")  // Http proxy address, set in Jenkins Credentials
        ARTIFACTORY_CREDS = "ARTIFACTORY_CREDENTIALS"
        ARTIFACTORY_PYPI_REPO = "LR_mbs-results"
        BUILD_BRANCH = "fix_jenkins" // only deploy from this/these branches
        BUILD_TAG = "v*.*.*" // only deploy with a commit message tagged like v0.0.1
    }

    // Don't use default checkout process, as we define it as a stage below
    options {
        skipDefaultCheckout true
    }

    // Agent must always be set at the top level of the pipeline
    agent any

    stages {
        stage("Checkout") {
            // We have to specify an appropriate slave for each stage
            // Choose from download, build, test, deploy
            agent { label "download.jenkins.slave" }
            steps {
                colourText("info", "Checking out code from source control.")
                checkout scm
		script {
                    buildInfo.name = "${PROJECT_NAME}"
                    buildInfo.number = "${BUILD_NUMBER}"
                    buildInfo.env.collect()
                }
                // Stash the files that have been checked out, for use in subsequent stages
                stash name: "Checkout", useDefaultExcludes: false
            }

        }
        stage("Build") {
            agent { label "build.${agentPython3Version}" }
            steps {
                unstash name: 'Checkout'
                colourText('info', "Building package")
                sh 'pip3 install setuptools'
                sh 'pip3 install wheel'
                sh 'python3 setup.py build bdist_wheel'
                stash name: "Build", useDefaultExcludes: false
            }
        }
        stage("Deploy") {
            when {
                anyOf{
                    branch BUILD_BRANCH
                    tag BUILD_TAG
                }
                beforeAgent true
            }
            agent { label "deploy.jenkins.slave" }
            steps {
                unstash name: "Build"
                colourText('info', "Deploying to Artifactory")
                pushToPyPiArtifactoryRepo("${buildInfo.name}")
            }
        }
    }
}
