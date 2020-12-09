@Library('jenkinsPipeline')_


properties([
  parameters([
     booleanParam(name: 'DEPLOY_TEMPLATES', defaultValue: false, description: 'Opt to deploy Dataflow and Airflow templates'),
     booleanParam(name: 'PERFORM_SCANS', defaultValue: true, description: 'Opt to perform Sonar, NexusIQ, Fortify scans'),
	 choice(name: 'ENV', choices: ['dev', 'qa', 'uat', 'prod'], description: 'Environment to be deployed')
  ])
])

def buildArguments(envArgs, templateArgs, gcsBucket){
    def arguments = ""
    envArgs.each { key, value ->
		value = value.replace("{{GCS_BUCKET}}", gcsBucket)
        arguments += "--${key}=${value} "
    }
    templateArgs.each { key, value ->
		value = value.replace("{{GCS_BUCKET}}", gcsBucket)
		arguments += "--${key}=${value} "
    }
    return arguments.trim();
}

def getconfig(envProps, env, branchName){
    if(branchName == "master"){
        return envProps[env]
    }else{
        return envProps[env]
    }
}

def validateEnvParam(env, branchName){
	if(env != null){
		if(branchName != "master" && (env == "uat" || env == "prod"))
			throw new Exception("Environment ${env} not allowed")
	}
	else{
		throw new Exception("null ENV param")
	}
}

if(params.PERFORM_SCANS && env.BRANCH_NAME == 'develop'){
    env.enableFortify=true
    echo 'enabled fortify scan....'
    standardGCPPipeline(email: 'sunilkumar.borusu@equifax.com')
}else if(params.PERFORM_SCANS){
    standardGCPPipeline(email: 'sunilkumar.borusu@equifax.com')
}else{
    echo 'Skipping scan stages.....'
}

node{
	if(params.DEPLOY_TEMPLATES){
		node("dibi-batch-pod") {
			try{
				def envProps = null
				stage("Validate Environment"){
					validateEnvParam(params.ENV, env.BRANCH_NAME)
				
				}
				stage("Initialize Environment"){
					checkout scm
					envProps = readJSON file: 'dataflow-config.json'
				}
				stage("Create Dataflow Template"){
					def envConfig = null
					envConfig = getconfig(envProps, params.ENV, env.BRANCH_NAME)
					container('mvn36jdk11') {
						configFileProvider([configFile(fileId: "defaultMVNSettings", variable: 'MAVEN_SETTINGS')])
						{
							def templates = envConfig['templates']
							def envArgs = envConfig['arguments']
							def gcsBucket = envConfig['gcsBucket']
							for(template in templates){
								def templateArgs = template['arguments']
								def mainClass = template['mainClass']
								def templateName = template['templateName']
								echo "Creating template ${templateName}......"
								def arguments = buildArguments(envArgs, templateArgs, gcsBucket)
								def files = findFiles(glob: '**/*bundled*.jar')
								sh "mvn -Pdataflow-runner compile exec:java -s $MAVEN_SETTINGS -Denv=${params.ENV} -Dexec.mainClass=${mainClass} -Dexec.args='${arguments}'"
							}
						}
					}
				}
				stage("Deploy SQL files"){
					def envConfig = null
					envConfig = getconfig(envProps, params.ENV, env.BRANCH_NAME)
					def sqlFiles = findFiles(glob: 'sql/*.sql')
					if(sqlFiles.size() > 0){
						def gcsBucket = envConfig['gcsBucket']
						def sqlFolder = envConfig['sqlConfig']['sqlFolder']
						sqlFolder = sqlFolder.replace("{{GCS_BUCKET}}", gcsBucket)
						sh "gsutil cp ./sql/*.sql ${sqlFolder}"
					}
					else{
						echo 'No SQL file found'
					}
				}
				stage("Deploy Airflow Templates"){
					if(findFiles(glob: 'airflow/*.py').size() > 0){
						container('mvn36jdk11') {
								configFileProvider([configFile(fileId: "defaultMVNSettings", variable: 'MAVEN_SETTINGS')])
								{
									sh "mvn replacer:replace -s $MAVEN_SETTINGS -Denv=${params.ENV}"
								}
							}
						def airflowTargetFiles = findFiles(glob: 'target/airflow/*.py')
						if(airflowTargetFiles.size() > 0){
							def envConfig = null
							envConfig = getconfig(envProps, params.ENV, env.BRANCH_NAME)
							sh "gsutil cp ./target/airflow/*.py ${envConfig['dagConfig']['dagsFolder']}"
						}
					}
					else{
						echo 'No airflow file found'
					}
				}
			}catch(e){
				echo 'Template Deployement failed'
                throw e;
			}finally{
				deleteDir()
			}
		}
	}else{
		echo 'Deploy template stages opted out...skipping deploy template stages'
	}
}

