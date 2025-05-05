"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Plus, Trash } from "lucide-react"

export default function ConfigForm() {
  const [connectors, setConnectors] = useState([{ id: "1", name: "", class: "", path: "", pipeline: "" }])
  const [pipelines, setPipelines] = useState([{ id: "1", name: "", stages: [{ id: "1", name: "", class: "" }] }])

  const addConnector = () => {
    setConnectors([...connectors, { id: Date.now().toString(), name: "", class: "", path: "", pipeline: "" }])
  }

  const addPipeline = () => {
    setPipelines([...pipelines, { id: Date.now().toString(), name: "", stages: [{ id: "1", name: "", class: "" }] }])
  }

  const addStage = (pipelineId: string) => {
    setPipelines(
      pipelines.map((pipeline) =>
        pipeline.id === pipelineId
          ? { ...pipeline, stages: [...pipeline.stages, { id: Date.now().toString(), name: "", class: "" }] }
          : pipeline,
      ),
    )
  }

  const removeConnector = (id: string) => {
    setConnectors(connectors.filter((connector) => connector.id !== id))
  }

  const removePipeline = (id: string) => {
    setPipelines(pipelines.filter((pipeline) => pipeline.id !== id))
  }

  const removeStage = (pipelineId: string, stageId: string) => {
    setPipelines(
      pipelines.map((pipeline) =>
        pipeline.id === pipelineId
          ? { ...pipeline, stages: pipeline.stages.filter((stage) => stage.id !== stageId) }
          : pipeline,
      ),
    )
  }

  return (
    <div className="space-y-8">
      <div className="space-y-4">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="space-y-2">
            <Label htmlFor="config-name">Configuration Name</Label>
            <Input id="config-name" placeholder="Enter configuration name" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="config-description">Description</Label>
            <Input id="config-description" placeholder="Enter description" />
          </div>
        </div>
      </div>

      <Tabs defaultValue="connectors">
        <TabsList className="mb-4">
          <TabsTrigger value="connectors">Connectors</TabsTrigger>
          <TabsTrigger value="pipelines">Pipelines</TabsTrigger>
          <TabsTrigger value="indexer">Indexer</TabsTrigger>
        </TabsList>

        <TabsContent value="connectors">
          <div className="space-y-4">
            <div className="flex justify-between items-center">
              <h3 className="text-lg font-medium">Connectors</h3>
              <Button onClick={addConnector} size="sm">
                <Plus className="mr-2 h-4 w-4" /> Add Connector
              </Button>
            </div>

            <Accordion type="multiple" className="space-y-4">
              {connectors.map((connector, index) => (
                <AccordionItem key={connector.id} value={connector.id} className="border rounded-md px-4">
                  <AccordionTrigger className="py-2">
                    <div className="flex items-center">
                      <span>Connector {index + 1}</span>
                      <span className="ml-2 text-sm text-muted-foreground">
                        {connector.name ? `- ${connector.name}` : ""}
                      </span>
                    </div>
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="space-y-4 py-2">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label htmlFor={`connector-name-${connector.id}`}>Name</Label>
                          <Input id={`connector-name-${connector.id}`} placeholder="Connector name" />
                        </div>
                        <div className="space-y-2">
                          <Label htmlFor={`connector-class-${connector.id}`}>Class</Label>
                          <Input id={`connector-class-${connector.id}`} placeholder="Connector class" />
                        </div>
                      </div>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label htmlFor={`connector-path-${connector.id}`}>Path</Label>
                          <Input id={`connector-path-${connector.id}`} placeholder="Data path" />
                        </div>
                        <div className="space-y-2">
                          <Label htmlFor={`connector-pipeline-${connector.id}`}>Pipeline</Label>
                          <Select>
                            <SelectTrigger id={`connector-pipeline-${connector.id}`}>
                              <SelectValue placeholder="Select pipeline" />
                            </SelectTrigger>
                            <SelectContent>
                              {pipelines.map((pipeline) => (
                                <SelectItem key={pipeline.id} value={pipeline.id}>
                                  {pipeline.name || `Pipeline ${pipeline.id}`}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                      </div>
                      <div className="pt-2 flex justify-end">
                        <Button variant="destructive" size="sm" onClick={() => removeConnector(connector.id)}>
                          <Trash className="mr-2 h-4 w-4" /> Remove
                        </Button>
                      </div>
                    </div>
                  </AccordionContent>
                </AccordionItem>
              ))}
            </Accordion>
          </div>
        </TabsContent>

        <TabsContent value="pipelines">
          <div className="space-y-4">
            <div className="flex justify-between items-center">
              <h3 className="text-lg font-medium">Pipelines</h3>
              <Button onClick={addPipeline} size="sm">
                <Plus className="mr-2 h-4 w-4" /> Add Pipeline
              </Button>
            </div>

            <Accordion type="multiple" className="space-y-4">
              {pipelines.map((pipeline, index) => (
                <AccordionItem key={pipeline.id} value={pipeline.id} className="border rounded-md px-4">
                  <AccordionTrigger className="py-2">
                    <div className="flex items-center">
                      <span>Pipeline {index + 1}</span>
                      <span className="ml-2 text-sm text-muted-foreground">
                        {pipeline.name ? `- ${pipeline.name}` : ""}
                      </span>
                    </div>
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="space-y-4 py-2">
                      <div className="space-y-2">
                        <Label htmlFor={`pipeline-name-${pipeline.id}`}>Name</Label>
                        <Input id={`pipeline-name-${pipeline.id}`} placeholder="Pipeline name" />
                      </div>

                      <div className="space-y-4">
                        <div className="flex justify-between items-center">
                          <h4 className="text-sm font-medium">Stages</h4>
                          <Button onClick={() => addStage(pipeline.id)} size="sm" variant="outline">
                            <Plus className="mr-2 h-4 w-4" /> Add Stage
                          </Button>
                        </div>

                        {pipeline.stages.map((stage, stageIndex) => (
                          <div key={stage.id} className="border rounded-md p-4 space-y-4">
                            <div className="flex justify-between items-center">
                              <h5 className="text-sm font-medium">Stage {stageIndex + 1}</h5>
                              <Button variant="ghost" size="sm" onClick={() => removeStage(pipeline.id, stage.id)}>
                                <Trash className="h-4 w-4" />
                              </Button>
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                              <div className="space-y-2">
                                <Label htmlFor={`stage-name-${pipeline.id}-${stage.id}`}>Name</Label>
                                <Input id={`stage-name-${pipeline.id}-${stage.id}`} placeholder="Stage name" />
                              </div>
                              <div className="space-y-2">
                                <Label htmlFor={`stage-class-${pipeline.id}-${stage.id}`}>Class</Label>
                                <Input id={`stage-class-${pipeline.id}-${stage.id}`} placeholder="Stage class" />
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>

                      <div className="pt-2 flex justify-end">
                        <Button variant="destructive" size="sm" onClick={() => removePipeline(pipeline.id)}>
                          <Trash className="mr-2 h-4 w-4" /> Remove
                        </Button>
                      </div>
                    </div>
                  </AccordionContent>
                </AccordionItem>
              ))}
            </Accordion>
          </div>
        </TabsContent>

        <TabsContent value="indexer">
          <div className="space-y-4">
            <h3 className="text-lg font-medium">Indexer Configuration</h3>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="indexer-type">Indexer Type</Label>
                <Select>
                  <SelectTrigger id="indexer-type">
                    <SelectValue placeholder="Select indexer type" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="elasticsearch">Elasticsearch</SelectItem>
                    <SelectItem value="solr">Solr</SelectItem>
                    <SelectItem value="opensearch">OpenSearch</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="indexer-url">Indexer URL</Label>
                <Input id="indexer-url" placeholder="http://localhost:9200" />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="index-name">Index Name</Label>
                <Input id="index-name" placeholder="Enter index name" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="batch-size">Batch Size</Label>
                <Input id="batch-size" type="number" placeholder="1000" />
              </div>
            </div>
          </div>
        </TabsContent>
      </Tabs>

      <div className="flex justify-end gap-4">
        <Button variant="outline">Cancel</Button>
        <Button>Save Configuration</Button>
      </div>
    </div>
  )
}

