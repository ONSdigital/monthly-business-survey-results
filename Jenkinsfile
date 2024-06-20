#!groovy

// Global scope required for multi-stage persistence
def artServer = Artifactory.server "art-p-01"
def buildInfo = Artifactory.newBuildInfo()
def agentPython3Version = 'python_3.6.1'


def pushToPyPiArtifactoryRepo(String projectName, String sourceDist = 'dist/*', String artifactoryHost = 'art-p-01') {
    withCredentials([usernamePassword(credentialsId: env.ARTIFACTORY_CREDS, usernameVariable: 'ARTIFACTORY_USER', passwordVariable: 'ARTIFACTORY_PASSWORD')]){
        sh "curl -u ${ARTIFACTORY_USER}:\${ARTIFACTORY_PASSWORD} -T ${sourceDist} 'http://${artifactoryHost}/artifactory/${env.ARTIFACTORY_PYPI_REPO}/${projectName}/'"
    }
}

pipeline {
    libraries {
            // Useful library from ONS internal GitLab
            lib('jenkins-pipeline-shared')
        }

    // Define environment variables
    environment {
        PROJECT_NAME = "monthly-business-survey-results"
        MAIN_BRANCH = "main"
        PROXY = credentials("PROXY")  // Http proxy address, set in Jenkins Credentials
        // Only need these if you're deploying code to Artifactory
        ARTIFACTORY_CREDS = credentials("ARTIFACTORY_CREDENTIALS")
        ARTIFACTORY_PYPI_REPO = "LR_mbs_results"
        BUILD_BRANCH = "main"
        BUILD_TAG = "*-release"
    }

    // Don't use default checkout process, as we define it as a stage below
    options {
        skipDefaultCheckout true
    }

    // Agent must always be set at the top level of the pipeline
    agent any

    stages {
        // Checkout stage to fetch code from  GitLab
        stage("Checkout") {
            // We have to specify an appropriate slave for each stage
            // Choose from download, build, test, deploy
            agent { label "download.jenkins.slave" }
            steps {
                colourText("info", "Checking out code from source control.")
                checkout scm
                // Stash the files that have been checked out, for use in subsequent stages
                stash name: "Checkout", useDefaultExcludes: false
            }

        }
        stage("Build") {
            agent { label "build.${agentPython3Version}" }
            steps {
                unstash name: 'Checkout'
                colourText('info', "Building package")
                sh 'pip3 install wheel==0.29.0'
                sh 'python3 setup.py build bdist_wheel'
                stash name: "Build", useDefaultExcludes: false
            }
        }
        //stage("Test") {
        //    agent { label "test.${agentPython3Version}" }
        //    steps {
        //        unstash name: "Checkout"
        //        colourText('info', "Running pytest.")                
        //        sh 'pip3 install pytest'
        //        sh 'python3 setup.py install'  // Possibly more realistic than `pip install -e .`
        //        sh 'python3 -m pytest'
        //    }
        //}
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
