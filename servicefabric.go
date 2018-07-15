package servicefabric

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/ido50/requests"
)

// DefaultAPIVersion is a default Service Fabric REST API version
const DefaultAPIVersion = "3.0"

type queryParamsFunc func(params []string) []string

type ApplicationItemsPage struct {
	ContinuationToken *string           `json:"ContinuationToken"`
	Items             []ApplicationItem `json:"Items"`
}

type AppParameter struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type ApplicationItem struct {
	HealthState string          `json:"HealthState"`
	ID          string          `json:"Id"`
	Name        string          `json:"Name"`
	Parameters  []*AppParameter `json:"Parameters"`
	Status      string          `json:"Status"`
	TypeName    string          `json:"TypeName"`
	TypeVersion string          `json:"TypeVersion"`
}

type ServiceItemsPage struct {
	ContinuationToken *string       `json:"ContinuationToken"`
	Items             []ServiceItem `json:"Items"`
}

type ServiceItem struct {
	HasPersistedState bool   `json:"HasPersistedState"`
	HealthState       string `json:"HealthState"`
	ID                string `json:"Id"`
	IsServiceGroup    bool   `json:"IsServiceGroup"`
	ManifestVersion   string `json:"ManifestVersion"`
	Name              string `json:"Name"`
	ServiceKind       string `json:"ServiceKind"`
	ServiceStatus     string `json:"ServiceStatus"`
	TypeName          string `json:"TypeName"`
}

type PartitionItemsPage struct {
	ContinuationToken *string         `json:"ContinuationToken"`
	Items             []PartitionItem `json:"Items"`
}

type PartitionItem struct {
	CurrentConfigurationEpoch ConfigurationEpoch   `json:"CurrentConfigurationEpoch"`
	HealthState               string               `json:"HealthState"`
	MinReplicaSetSize         int64                `json:"MinReplicaSetSize"`
	PartitionInformation      PartitionInformation `json:"PartitionInformation"`
	PartitionStatus           string               `json:"PartitionStatus"`
	ServiceKind               string               `json:"ServiceKind"`
	TargetReplicaSetSize      int64                `json:"TargetReplicaSetSize"`
}

type ConfigurationEpoch struct {
	ConfigurationVersion string `json:"ConfigurationVersion"`
	DataLossVersion      string `json:"DataLossVersion"`
}

type PartitionInformation struct {
	HighKey              string `json:"HighKey"`
	ID                   string `json:"Id"`
	LowKey               string `json:"LowKey"`
	ServicePartitionKind string `json:"ServicePartitionKind"`
}

type ReplicaItemBase struct {
	Address                      string `json:"Address"`
	HealthState                  string `json:"HealthState"`
	LastInBuildDurationInSeconds string `json:"LastInBuildDurationInSeconds"`
	NodeName                     string `json:"NodeName"`
	ReplicaRole                  string `json:"ReplicaRole"`
	ReplicaStatus                string `json:"ReplicaStatus"`
	ServiceKind                  string `json:"ServiceKind"`
}

type ReplicaItemsPage struct {
	ContinuationToken *string       `json:"ContinuationToken"`
	Items             []ReplicaItem `json:"Items"`
}

type ReplicaItem struct {
	*ReplicaItemBase
	ID string `json:"ReplicaId"`
}

func (m *ReplicaItem) GetReplicaData() (string, *ReplicaItemBase) {
	return m.ID, m.ReplicaItemBase
}

type InstanceItemsPage struct {
	ContinuationToken *string        `json:"ContinuationToken"`
	Items             []InstanceItem `json:"Items"`
}

type InstanceItem struct {
	*ReplicaItemBase
	ID string `json:"InstanceId"`
}

func (m *InstanceItem) GetReplicaData() (string, *ReplicaItemBase) {
	return m.ID, m.ReplicaItemBase
}

type ServiceType struct {
	ServiceTypeDescription ServiceTypeDescription `json:"ServiceTypeDescription"`
	ServiceManifestVersion string                 `json:"ServiceManifestVersion"`
	ServiceManifestName    string                 `json:"ServiceManifestName"`
	IsServiceGroup         bool                   `json:"IsServiceGroup"`
}

type ServiceTypeDescription struct {
	IsStateful               bool           `json:"IsStateful"`
	ServiceTypeName          string         `json:"ServiceTypeName"`
	PlacementConstraints     string         `json:"PlacementConstraints"`
	HasPersistedState        bool           `json:"HasPersistedState"`
	Kind                     string         `json:"Kind"`
	Extensions               []KeyValuePair `json:"Extensions"`
	LoadMetrics              []interface{}  `json:"LoadMetrics"`
	ServicePlacementPolicies []interface{}  `json:"ServicePlacementPolicies"`
}

type PropertiesListPage struct {
	ContinuationToken string     `json:"ContinuationToken"`
	IsConsistent      bool       `json:"IsConsistent"`
	Properties        []Property `json:"Properties"`
}

type Property struct {
	Metadata Metadata  `json:"Metadata"`
	Name     string    `json:"Name"`
	Value    PropValue `json:"Value"`
}

type Metadata struct {
	CustomTypeID             string `json:"CustomTypeId"`
	LastModifiedUtcTimestamp string `json:"LastModifiedUtcTimestamp"`
	Parent                   string `json:"Parent"`
	SequenceNumber           string `json:"SequenceNumber"`
	SizeInBytes              int64  `json:"SizeInBytes"`
	TypeID                   string `json:"TypeId"`
}

type PropValue struct {
	Data string `json:"Data"`
	Kind string `json:"Kind"`
}

type KeyValuePair struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type ServiceExtensionLabels struct {
	XMLName xml.Name `xml:"Labels"`
	Label   []struct {
		XMLName xml.Name `xml:"Label"`
		Value   string   `xml:",chardata"`
		Key     string   `xml:"Key,attr"`
	}
}

// Client for Service Fabric.
type ServiceFabricClient struct {
	// endpoint Service Fabric cluster management endpoint
	endpoint string
	// apiVersion Service Fabric API version
	apiVersion string
	// httpClient HTTP client
	httpClient *requests.HTTPClient
}

func NewServiceFabricClient(httpClient *requests.HTTPClient, endpoint, apiVersion string) (*ServiceFabricClient, error) {
	if endpoint == "" {
		return nil, errors.New("endpoint missing for httpClient configuration")
	}
	if apiVersion == "" {
		apiVersion = DefaultAPIVersion
	}

	return &ServiceFabricClient{
		endpoint:   endpoint,
		apiVersion: apiVersion,
		httpClient: httpClient,
	}, nil
}

func (c ServiceFabricClient) GetApplications() (*ApplicationItemsPage, error) {
	var aggregateAppItemsPages ApplicationItemsPage
	var continueToken string
	for {
		res, err := c.getHTTP("Applications/", withContinue(continueToken))
		if err != nil {
			return nil, err
		}

		var appItemsPage ApplicationItemsPage
		err = json.Unmarshal(res, &appItemsPage)
		if err != nil {
			return nil, fmt.Errorf("could not deserialise JSON response: %+v", err)
		}

		aggregateAppItemsPages.Items = append(aggregateAppItemsPages.Items, appItemsPage.Items...)

		continueToken = getString(appItemsPage.ContinuationToken)
		if continueToken == "" {
			break
		}
	}
	return &aggregateAppItemsPages, nil
}

func (c ServiceFabricClient) GetServices(appName string) (*ServiceItemsPage, error) {
	var aggregateServiceItemsPages ServiceItemsPage
	var continueToken string
	for {
		res, err := c.getHTTP("Applications/"+appName+"/$/GetServices", withContinue(continueToken))
		if err != nil {
			return nil, err
		}

		var servicesItemsPage ServiceItemsPage
		err = json.Unmarshal(res, &servicesItemsPage)
		if err != nil {
			return nil, fmt.Errorf("could not deserialise JSON response: %+v", err)
		}

		aggregateServiceItemsPages.Items = append(aggregateServiceItemsPages.Items, servicesItemsPage.Items...)

		continueToken = getString(servicesItemsPage.ContinuationToken)
		if continueToken == "" {
			break
		}
	}
	return &aggregateServiceItemsPages, nil
}

func (c ServiceFabricClient) GetClusterHealth() (bool, error) {
	res, err := c.getHTTPRaw("/$/GetClusterHealth?api-version=6.0&")
	if err != nil {
		return false, fmt.Errorf("error getting cluster health")
	}

	return res == http.StatusOK, nil
}

func (c ServiceFabricClient) DeleteService(serviceId string) (bool, error) {
	res, err := c.postHTTP("/Services/" + serviceId + "/$/Delete",withParam("api-version",c.apiVersion))
	if err != nil {
		return false, fmt.Errorf("error getting cluster health")
	}

	return res == http.StatusOK, nil
}

func (c ServiceFabricClient) DeleteApplication(applicationId string) (bool, error) {
	res, err := c.postHTTP("/Applications/" + applicationId + "/$/Delete",withParam("api-version",c.apiVersion))
	if err != nil {
		return false, fmt.Errorf("error getting cluster health")
	}

	return res == http.StatusOK, nil
}

func (c ServiceFabricClient) GetServiceExtension(appType, applicationVersion, serviceTypeName, extensionKey string, response interface{}) error {
	res, err := c.getHTTP("ApplicationTypes/"+appType+"/$/GetServiceTypes", withParam("ApplicationTypeVersion", applicationVersion))
	if err != nil {
		return fmt.Errorf("error requesting service extensions: %v", err)
	}

	var serviceTypes []ServiceType
	err = json.Unmarshal(res, &serviceTypes)
	if err != nil {
		return fmt.Errorf("could not deserialise JSON response: %+v", err)
	}

	for _, serviceTypeInfo := range serviceTypes {
		if serviceTypeInfo.ServiceTypeDescription.ServiceTypeName == serviceTypeName {
			for _, extension := range serviceTypeInfo.ServiceTypeDescription.Extensions {
				if strings.EqualFold(extension.Key, extensionKey) {
					err = xml.Unmarshal([]byte(extension.Value), &response)
					if err != nil {
						return fmt.Errorf("could not deserialise extension's XML value: %+v", err)
					}
					return nil
				}
			}
		}
	}
	return nil
}

func (c ServiceFabricClient) GetServiceExtensionMap(service *ServiceItem, app *ApplicationItem, extensionKey string) (map[string]string, error) {
	extensionData := ServiceExtensionLabels{}
	err := c.GetServiceExtension(app.TypeName, app.TypeVersion, service.TypeName, extensionKey, &extensionData)
	if err != nil {
		return nil, err
	}

	labels := map[string]string{}
	if extensionData.Label != nil {
		for _, label := range extensionData.Label {
			labels[label.Key] = label.Value
		}
	}

	return labels, nil
}

func (c ServiceFabricClient) GetProperties(name string) (bool, map[string]string, error) {
	nameExists, err := c.nameExists(name)
	if err != nil {
		return false, nil, err
	}

	if !nameExists {
		return false, nil, nil
	}

	properties := make(map[string]string)

	var continueToken string
	for {
		res, err := c.getHTTP("Names/"+name+"/$/GetProperties", withContinue(continueToken), withParam("IncludeValues", "true"))
		if err != nil {
			return false, nil, err
		}

		var propertiesListPage PropertiesListPage
		err = json.Unmarshal(res, &propertiesListPage)
		if err != nil {
			return false, nil, fmt.Errorf("could not deserialise JSON response: %+v", err)
		}

		for _, property := range propertiesListPage.Properties {
			if property.Value.Kind != "String" {
				continue
			}
			properties[property.Name] = property.Value.Data
		}

		continueToken = propertiesListPage.ContinuationToken
		if continueToken == "" {
			break
		}
	}

	return true, properties, nil
}

func (c ServiceFabricClient) nameExists(propertyName string) (bool, error) {
	res, err := c.getHTTPRaw("Names/" + propertyName)
	// Get http will return error for any non 200 response code.
	if err != nil {
		return false, err
	}

	return res == http.StatusOK, nil
}

func (c ServiceFabricClient) getHTTP(basePath string, paramsFuncs ...queryParamsFunc) ([]byte, error) {
	if c.httpClient == nil {
		return nil, errors.New("invalid http client provided")
	}

	var text string
	var status int
	url := c.getURL(basePath, paramsFuncs...)
	err := c.httpClient.NewRequest("GET", url).Into(&text).
		StatusInto(&status).
		Run()

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Service Fabric server %+v on %s", err, url)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("Service Fabric responded with error code %d to request %s with body",
			status, url)
	}

	if len(text) == 0 {
		return nil, errors.New("empty response body from Service Fabric")
	}

	return []byte(text), nil
}

func (c ServiceFabricClient) getHTTPRaw(basePath string) (int, error) {
	if c.httpClient == nil {
		return -1, fmt.Errorf("invalid http client provided")
	}

	url := c.getURL(basePath)

	var text string
	var status int
	err := c.httpClient.NewRequest("GET", url).Into(&text).
		StatusInto(&status).
		Run()
	if err != nil {
		return -1, fmt.Errorf("failed to connect to Service Fabric server %+v on %s", err, url)
	}
	return status, nil
}

func (c ServiceFabricClient) getURL(basePath string, paramsFuncs ...queryParamsFunc) string {
	params := []string{"api-version=" + c.apiVersion}

	for _, paramsFunc := range paramsFuncs {
		params = paramsFunc(params)
	}

	return fmt.Sprintf("%s/%s?%s", c.endpoint, basePath, strings.Join(params, "&"))
}

func (c ServiceFabricClient) postHTTP(basePath string,body []byte,paramsFuncs ...queryParamsFunc)([]byte,error){
	if c.httpClient == nil {
		return nil, errors.New("invalid http client provided")
	}

	url := c.getURL(basePath,paramsFuncs...)
	var body interface{}
	err := c.httpClient.NewRequest("POST", url).Into(&body).
		StatusInto(&status).
		Run()

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Service Fabric server %+v on %s", err, url)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("Service Fabric responded with error code %d to request %s with body",
			status, url)
	}

	if len(body) == 0 {
		return nil, errors.New("empty response body from Service Fabric")
	}

	return []byte(body), nil

}
func getString(str *string) string {
	if str == nil {
		return ""
	}
	return *str
}

func withContinue(token string) queryParamsFunc {
	if len(token) == 0 {
		return noOp
	}
	return withParam("continue", token)
}

func withParam(name, value string) queryParamsFunc {
	return func(params []string) []string {
		return append(params, name+"="+value)
	}
}

func noOp(params []string) []string {
	return params
}
